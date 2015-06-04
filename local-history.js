"use strict";

var _ = require('lodash');
var wait = require('co-wait');
var KindaObject = require('kinda-object');

var LocalHistory = KindaObject.extend('LocalHistory', function() {
  this.setCreator(function(options) {
    if (!options) options = {};

    var projection = options.projection || [];
    if (!_.isArray(projection)) projection = [projection];
    this.projection = projection;

    var excludedCollections = options.excludedCollections || [];
    if (!_.isArray(excludedCollections)) excludedCollections = [excludedCollections];
    this.excludedCollections = excludedCollections;
  });

  this.plug = function(repository) {
    var that = this;

    that.repository = repository;
    repository.history = that;

    that.primaryKeyIndexPrefix = [repository.name, '$History:primaryKey'];
    that.sequenceNumberIndexPrefix = [repository.name, '$History:sequenceNumber'];

    repository.onAsync('didPutItem', function *(item, options) {
      yield that.updateItem(this, item, 'put', options);
    });

    repository.onAsync('didDeleteItem', function *(item, options) {
      yield that.updateItem(this, item, 'delete', options);
    });
  };

  this.getLastSequenceNumber = function *(repository) {
    if (!repository) repository = this.repository;
    var repositoryId = yield repository.getRepositoryId();
    var record = yield repository.loadRepositoryRecord();
    var lastSequenceNumber = record.lastHistorySequenceNumber || 0;
    return {
      repositoryId: repositoryId,
      lastSequenceNumber: lastSequenceNumber
    };
  };

  this.incrementSequenceNumber = function *(repository) {
    if (!repository) repository = this.repository;
    // This 'isIncrementingSequenceNumber' semaphore should not be necessary
    // if the transaction system was better
    while (this._isIncrementingSequenceNumber) yield wait(50);
    try {
      this._isIncrementingSequenceNumber = true;
      var record = yield repository.loadRepositoryRecord();
      if (!record.hasOwnProperty('lastHistorySequenceNumber')) {
        record.lastHistorySequenceNumber = 0;
      }
      record.lastHistorySequenceNumber++;
      yield repository.saveRepositoryRecord(record);
      return record.lastHistorySequenceNumber;
    } finally {
      this._isIncrementingSequenceNumber = false;
    }
  };

  this.updateItem = function *(repository, item, operation, options) {
    if (!repository) repository = this.repository;
    if (!options) options = {};

    var collectionName = item.getCollection().getClassName();
    if (_.includes(this.excludedCollections, collectionName)) return;

    if (options.source === 'computer') return;

    if (!repository.isInsideTransaction()) {
      throw new Error('current repository should be inside a transaction');
    }

    var repositoryId = yield repository.getRepositoryId();
    var originRepositoryId = options.originRepositoryId || repositoryId;

    var store = repository.getStore();

    var primaryKey = item.getPrimaryKeyValue();
    var primaryKeyIndexKey = this.makePrimaryKeyIndexKey(primaryKey);

    var primaryKeyIndexValue = yield store.get(
      primaryKeyIndexKey, { errorIfMissing: false }
    );

    if (primaryKeyIndexValue) { // remove previous item
      var oldSequenceNumber = primaryKeyIndexValue.sequenceNumber;
      var oldSequenceNumberIndexKey = this.makeSequenceNumberIndexKey(oldSequenceNumber);
      yield store.del(oldSequenceNumberIndexKey);
    }

    if (options.source === 'localSynchronizer') {
      // in case the item comes from the local synchronizer
      // we must remove it from the local history
      if (primaryKeyIndexValue) yield store.del(primaryKeyIndexKey);
      return;
    }

    var newSequenceNumber = yield this.incrementSequenceNumber(repository);

    // update primaryKey index
    primaryKeyIndexValue = { sequenceNumber: newSequenceNumber };
    yield store.put(primaryKeyIndexKey, primaryKeyIndexValue);

    // update sequenceNumber index
    var newSequenceNumberIndexKey = this.makeSequenceNumberIndexKey(newSequenceNumber);
    var value = { primaryKey: primaryKey };
    if (operation === 'delete') value.isDeleted = true;
    value.originRepositoryId = originRepositoryId;
    this.projection.forEach(function(propertyKey) {
      var propertyValue = item[propertyKey];
      if (propertyValue != null) {
        if (!value.projection) value.projection = {};
        value.projection[propertyKey] = propertyValue;
      }
    });
    yield store.put(newSequenceNumberIndexKey, value, { errorIfExists: true });
  };

  this.findItemsAfterSequenceNumber = function *(sequenceNumber, options) {
    if (sequenceNumber == null) sequenceNumber = 0;
    if (!options) options = {};
    var result = yield this.getLastSequenceNumber(this.repository);
    var lastSequenceNumber = result.lastSequenceNumber;
    var items = [];
    if (lastSequenceNumber > sequenceNumber) { // OPTIMIZATION
      var store = this.repository.getStore();
      var results = yield store.getRange({
        prefix: this.sequenceNumberIndexPrefix,
        startAfter: sequenceNumber,
        end: lastSequenceNumber,
        limit: 100000 // TODO: implement forEach in the store and use it here
      });
      var filter = options.filter;
      if (filter && !_.isArray(filter)) filter = [filter];
      var ignoreOriginRepositoryId = options.ignoreOriginRepositoryId;
      results.forEach(function(result) {
        var value = result.value;
        if (ignoreOriginRepositoryId) {
          if (value.originRepositoryId === ignoreOriginRepositoryId) return;
        }
        if (filter) {
          var projection = value.projection;
          var isOkay = _.some(filter, function(condition) {
            for (var key in condition) {
              if (!condition.hasOwnProperty(key)) continue;
              if (key === '$primaryKey') {
                if (value.primaryKey !== condition.$primaryKey) return false;
              } else {
                if (!projection) return false;
                if (projection[key] !== condition[key]) return false;
              }
            }
            return true;
          });
          if (!isOkay) return;
        }
        items.push(_.omit(value, 'projection'));
      });
    }
    var repositoryId = yield this.repository.getRepositoryId();
    return {
      repositoryId: repositoryId,
      lastSequenceNumber: lastSequenceNumber,
      items: items
    };
  };

  this.deleteItemsUntilSequenceNumber = function *(sequenceNumber) {
    var store = this.repository.getStore();
    var results = yield store.getRange({
      prefix: this.sequenceNumberIndexPrefix,
      end: sequenceNumber,
      limit: 100000 // TODO: make it work for an unlimited number of items
    });
    var primaryKeyIndexKeys = results.map(function(result) {
      return this.makePrimaryKeyIndexKey(result.value.primaryKey);
    }, this);
    for (var i = 0; i < primaryKeyIndexKeys.length; i++) {
      // TODO: implement delMany in the store and use it here
      var primaryKeyIndexKey = primaryKeyIndexKeys[i];
      yield store.del(primaryKeyIndexKey);
    }
    yield store.delRange({
      prefix: this.sequenceNumberIndexPrefix,
      end: sequenceNumber
    });
  };

  this.getStatistics = function *() {
    var result = yield this.getLastSequenceNumber();
    var lastSequenceNumber = result.lastSequenceNumber;
    var store = this.repository.getStore();
    var primaryKeyIndexesCount = yield store.getCount({
      prefix: this.primaryKeyIndexPrefix
    });
    var sequenceNumberIndexesCount = yield store.getCount({
      prefix: this.sequenceNumberIndexPrefix
    });
    return {
      lastSequenceNumber: lastSequenceNumber,
      primaryKeyIndexesCount: primaryKeyIndexesCount,
      sequenceNumberIndexesCount: sequenceNumberIndexesCount
    };
  };

  this.forgetDeletedItems = function *() {
    var store = this.repository.getStore();
    var results = yield store.getRange({
      prefix: this.sequenceNumberIndexPrefix,
      limit: 100000 // TODO: implement forEach in the store and use it here
    });
    for (var i = 0; i < results.length; i++) {
      var result = results[i];
      if (result.value.isDeleted) {
        yield store.del(result.key);
        var primaryKeyIndexKey = this.makePrimaryKeyIndexKey(result.value.primaryKey);
        yield store.del(primaryKeyIndexKey);
      }
    }
  };

  // === Helpers ===

  this.makePrimaryKeyIndexKey = function(primaryKey) {
    return this.primaryKeyIndexPrefix.concat(primaryKey);
  };

  this.makeSequenceNumberIndexKey = function(sequenceNumber) {
    return this.sequenceNumberIndexPrefix.concat(sequenceNumber);
  };
});

module.exports = LocalHistory;
