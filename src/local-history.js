'use strict';

let _ = require('lodash');
let wait = require('co-wait');
let KindaObject = require('kinda-object');

let LocalHistory = KindaObject.extend('LocalHistory', function() {
  this.creator = function(options = {}) {
    let projection = options.projection || [];
    if (!_.isArray(projection)) projection = [projection];
    this.projection = projection;

    let excludedCollections = options.excludedCollections || [];
    if (!_.isArray(excludedCollections)) excludedCollections = [excludedCollections];
    this.excludedCollections = excludedCollections;
  };

  this.plug = function(repository) {
    let that = this;

    that.repository = repository;
    repository.history = that;

    that.primaryKeyIndexPrefix = [repository.name, '$History:primaryKey'];
    that.sequenceNumberIndexPrefix = [repository.name, '$History:sequenceNumber'];

    repository.onAsync('didPutItem', function *(item, options) {
      yield that.updateItem(item, 'put', options);
    });

    repository.onAsync('didDeleteItem', function *(item, options) {
      yield that.updateItem(item, 'delete', options);
    });
  };

  this.getLastSequenceNumber = function *(repository = this.repository) {
    let repositoryId = yield repository.getRepositoryId();
    let record = yield repository.loadRepositoryRecord();
    let lastSequenceNumber = record.lastHistorySequenceNumber || 0;
    return { repositoryId, lastSequenceNumber };
  };

  this.incrementSequenceNumber = function *(repository = this.repository) {
    // This 'isIncrementingSequenceNumber' semaphore should not be necessary
    // if the transaction system was better
    while (this._isIncrementingSequenceNumber) yield wait(50);
    try {
      this._isIncrementingSequenceNumber = true;
      let record = yield repository.loadRepositoryRecord();
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

  this.updateItem = function *(item, operation, options = {}) {
    let collectionName = item.collection.class.name;
    if (_.includes(this.excludedCollections, collectionName)) return;

    if (options.source === 'computer') return;

    let repository = item.collection.repository;

    if (!repository.isInsideTransaction) {
      throw new Error('current repository should be inside a transaction');
    }

    let repositoryId = yield repository.getRepositoryId();
    let originRepositoryId = options.originRepositoryId || repositoryId;

    let store = repository.store;

    let primaryKey = item.primaryKeyValue;
    let primaryKeyIndexKey = this.makePrimaryKeyIndexKey(primaryKey);

    let primaryKeyIndexValue = yield store.get(
      primaryKeyIndexKey, { errorIfMissing: false }
    );

    if (primaryKeyIndexValue) { // remove previous item
      let oldSequenceNumber = primaryKeyIndexValue.sequenceNumber;
      let oldSequenceNumberIndexKey = this.makeSequenceNumberIndexKey(oldSequenceNumber);
      let hasBeenDeleted = yield store.del(
        oldSequenceNumberIndexKey, { errorIfMissing: false }
      );
      if (!hasBeenDeleted) {
        repository.log.warning('in the local repository history, a sequence number index was not found while trying to delete it');
      }
    }

    if (options.source === 'localSynchronizer') {
      // in case the item comes from the local synchronizer
      // we must remove it from the local history
      if (primaryKeyIndexValue) {
        let hasBeenDeleted = yield store.del(
          primaryKeyIndexKey, { errorIfMissing: false }
        );
        if (!hasBeenDeleted) {
          repository.log.warning('in the local repository history, a primary key index was not found while trying to delete it');
        }
      }
      return;
    }

    let newSequenceNumber = yield this.incrementSequenceNumber(repository);

    // update primaryKey index
    primaryKeyIndexValue = { sequenceNumber: newSequenceNumber };
    yield store.put(primaryKeyIndexKey, primaryKeyIndexValue);

    // update sequenceNumber index
    let newSequenceNumberIndexKey = this.makeSequenceNumberIndexKey(newSequenceNumber);
    let value = { primaryKey };
    if (operation === 'delete') value.isDeleted = true;
    value.originRepositoryId = originRepositoryId;
    this.projection.forEach(propertyKey => {
      let propertyValue = item[propertyKey];
      if (propertyValue != null) {
        if (!value.projection) value.projection = {};
        value.projection[propertyKey] = propertyValue;
      }
    });
    try {
      yield store.put(newSequenceNumberIndexKey, value, { errorIfExists: true });
    } catch (err) {
      repository.log.error(err);
      repository.log.warning('in the local repository history, an error occured while trying to put a new sequence number index');
    }
  };

  this.findItemsAfterSequenceNumber = function *(sequenceNumber = 0, options = {}) {
    let result = yield this.getLastSequenceNumber(this.repository);
    let lastSequenceNumber = result.lastSequenceNumber;
    let items = [];
    if (lastSequenceNumber > sequenceNumber) { // OPTIMIZATION
      let store = this.repository.store;
      let results = yield store.getRange({
        prefix: this.sequenceNumberIndexPrefix,
        startAfter: sequenceNumber,
        end: lastSequenceNumber,
        limit: 100000 // TODO: implement forEach in the store and use it here
      });
      let filter = options.filter;
      if (filter && !_.isArray(filter)) filter = [filter];
      let ignoreOriginRepositoryId = options.ignoreOriginRepositoryId;
      results.forEach(res => {
        let value = res.value;
        if (ignoreOriginRepositoryId) {
          if (value.originRepositoryId === ignoreOriginRepositoryId) return;
        }
        if (filter) {
          let projection = value.projection;
          let isOkay = _.some(filter, condition => {
            for (let key in condition) {
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
    let repositoryId = yield this.repository.getRepositoryId();
    return { repositoryId, lastSequenceNumber, items };
  };

  this.deleteItemsUntilSequenceNumber = function *(sequenceNumber) {
    let store = this.repository.store;
    let results = yield store.getRange({
      prefix: this.sequenceNumberIndexPrefix,
      end: sequenceNumber,
      limit: 100000 // TODO: make it work for an unlimited number of items
    });
    let primaryKeyIndexKeys = results.map(result => {
      return this.makePrimaryKeyIndexKey(result.value.primaryKey);
    });
    for (let primaryKeyIndexKey of primaryKeyIndexKeys) {
      yield store.del(primaryKeyIndexKey);
    }
    yield store.delRange({
      prefix: this.sequenceNumberIndexPrefix,
      end: sequenceNumber
    });
  };

  this.getStatistics = function *() {
    let result = yield this.getLastSequenceNumber();
    let lastSequenceNumber = result.lastSequenceNumber;
    let store = this.repository.store;
    let primaryKeyIndexesCount = yield store.getCount({
      prefix: this.primaryKeyIndexPrefix
    });
    let sequenceNumberIndexesCount = yield store.getCount({
      prefix: this.sequenceNumberIndexPrefix
    });
    return {
      lastSequenceNumber,
      primaryKeyIndexesCount,
      sequenceNumberIndexesCount
    };
  };

  this.forgetDeletedItems = function *() {
    let store = this.repository.store;
    let results = yield store.getRange({
      prefix: this.sequenceNumberIndexPrefix,
      limit: 100000 // TODO: implement forEach in the store and use it here
    });
    for (let result of results) {
      if (result.value.isDeleted) {
        yield store.del(result.key);
        let primaryKeyIndexKey = this.makePrimaryKeyIndexKey(result.value.primaryKey);
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
