"use strict";

var _ = require('lodash');
var co = require('co');
var wait = require('co-wait');
var KindaObject = require('kinda-object');
var log = require('kinda-log').create();
var util = require('kinda-util').create();
var Connectivity = require('kinda-connectivity');
var LocalHistory = require('./local-history');
var RemoteHistory = require('./remote-history');

var KindaRepositorySynchronizer = KindaObject.extend('KindaRepositorySynchronizer', function() {
  this.setCreator(function(localRepository, remoteRepository, options) {
    if (!options) options = {};

    if (!localRepository.history) {
      localRepository.use(LocalHistory.create(options));
    }
    if (!remoteRepository.history) {
      remoteRepository.use(RemoteHistory.create(options));
    }

    this.localRepository = localRepository;
    localRepository.synchronizer = this;
    this.remoteRepository = remoteRepository;
    remoteRepository.synchronizer = this;
    if (options.filter) this.setFilter(options.filter);

    localRepository.onAsync('willDestroy', function *() {
      yield this.suspend();
    }.bind(this));

    localRepository.onAsync('didDestroy', function *() {
      this.hasBeenInitialized = false;
      delete this._remoteRepositoryId;
      delete this._remoteHistoryLastSequenceNumber;
      yield this.resume();
    }.bind(this));

    this.authorizationIsRequired = (
      options.authorizationIsRequired != null ?
      options.authorizationIsRequired : true
    );

    this.filterIsRequired = options.filterIsRequired;

    var pingURL = remoteRepository.baseURL;
    if (!_.endsWith(pingURL, '/')) pingURL += '/';
    pingURL += 'ping';
    this.connectivity = Connectivity.create(pingURL);
    this.connectivity.monitor();
  });

  this.getFilter = function() {
    return this._filter;
  };

  this.setFilter = function(filter) {
    this._filter = filter;
  };

  this.getRemoteRepositoryId = function *() {
    if (this._remoteRepositoryId) return this._remoteRepositoryId;
    var record = yield this.localRepository.loadRepositoryRecord();
    var remoteRepositoryId = record.remoteRepositoryId;
    if (!remoteRepositoryId) return;
    this._remoteRepositoryId = remoteRepositoryId;
    return remoteRepositoryId;
  };

  this.setRemoteRepositoryId = function *(remoteRepositoryId) {
    var record = yield this.localRepository.loadRepositoryRecord();
    record.remoteRepositoryId = remoteRepositoryId;
    yield this.localRepository.saveRepositoryRecord(record);
    this._remoteRepositoryId = remoteRepositoryId;
  };

  this.getRemoteHistoryLastSequenceNumber = function *() {
    if (this._remoteHistoryLastSequenceNumber != null) {
      return this._remoteHistoryLastSequenceNumber;
    }
    var record = yield this.localRepository.loadRepositoryRecord();
    var sequenceNumber = record.remoteHistoryLastSequenceNumber;
    if (sequenceNumber == null) return;
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
    return sequenceNumber;
  };

  this.setRemoteHistoryLastSequenceNumber = function *(sequenceNumber) {
    var record = yield this.localRepository.loadRepositoryRecord();
    record.remoteHistoryLastSequenceNumber = sequenceNumber;
    yield this.localRepository.saveRepositoryRecord(record);
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
  };

  this.initializeSynchronizer = function *() {
    if (this.hasBeenInitialized) return;
    var repositoryId = yield this.getRemoteRepositoryId();
    if (!repositoryId) {
      repositoryId = yield this.remoteRepository.getRepositoryId();
      yield this.setRemoteRepositoryId(repositoryId);
    }
    var sequenceNumber = yield this.getRemoteHistoryLastSequenceNumber();
    if (sequenceNumber == null) {
      sequenceNumber = 0;
      yield this.setRemoteHistoryLastSequenceNumber(sequenceNumber);
    }
    this.hasBeenInitialized = true;
    yield this.emitAsync('didInitialize');
  };

  this.start = function() {
    co(function *() {
      if (this._isStarted) return;
      this._isStarted = true;
      this._isStopping = false;
      try {
        this.emit('didStart');
        while (!this._isStopping) {
          try {
            yield this.run(true);
          } catch (err) {
            log.error(err);
          }
          if (!this._isStopping) {
            this._timeout = util.createTimeout(30 * 1000); // 30 seconds
            yield this._timeout.start();
            this._timeout = undefined;
          }
        }
      } finally {
        this._isStarted = false;
        this._isStopping = false;
      }
      this.emit('didStop');
    }.bind(this)).catch(function(err) {
      log.error(err.stack);
    });
  };

  this.stop = function() {
    if (!this._isStarted) return;
    if (this._isStopping) return;
    this._isStopping = true;
    if (this._timeout) this._timeout.stop();
    this.emit('willStop');
  };

  this.waitStop = function *() {
    while (this._isStarted || this._isRunning) {
      yield wait(100);
    }
  };

  this.suspend = function *() {
    this._isSuspended = true;
    while (this._isRunning) {
      yield wait(100);
    }
  };

  this.resume = function *() {
    this._isSuspended = false;
  };

  this.getIsStarted = function() {
    return this._isStarted;
  };

  this.getLastSynchronizationDate = function() {
    return this._lastSynchronizationDate;
  };

  this.run = function *(quietMode) {
    var stats = {};
    if (this._isRunning) return stats;
    if (this._isSuspended) return stats;
    if (this.authorizationIsRequired && !this.remoteRepository.getAuthorization()) {
      if (!quietMode) {
        log.notice('an authorization is required to run the synchronizer');
      }
      return stats;
    }
    if (this.filterIsRequired && !this.getFilter()) {
      if (!quietMode) {
        log.notice('a filter is required to run the synchronizer');
      }
      return stats;
    }
    if (this.connectivity.isOffline == null) yield this.connectivity.ping();
    if (this.connectivity.isOffline) {
      if (!quietMode) {
        log.notice('a working connection is required to run the synchronizer');
      }
      return stats;
    }
    try {
      this._isRunning = true;
      var remoteRepositoryId = yield this.getRemoteRepositoryId();
      var isFirstSynchronization = !remoteRepositoryId;
      this.emit('willRun', isFirstSynchronization);
      yield this.initializeSynchronizer();
      var localStats = yield this.receiveRemoteItems();
      var remoteStats = yield this.sendLocalItems();
      stats = {
        updatedLocalItemsCount: localStats.updatedItemsCount,
        deletedLocalItemsCount: localStats.deletedItemsCount,
        updatedRemoteItemsCount: remoteStats.updatedItemsCount,
        deletedRemoteItemsCount: remoteStats.deletedItemsCount
      };
      this._lastSynchronizationDate = new Date();
      this.emit('didRun', stats);
    } catch (err) {
      this.emit('didFail');
      throw err;
    } finally {
      this._isRunning = false;
    }
    return stats;
  };

  this.receiveRemoteItems = function *() {
    var result = yield this.getRemoteItems();
    var stats = yield this.saveRemoteItemsInLocalRepository(result.items);
    yield this.setRemoteHistoryLastSequenceNumber(result.lastSequenceNumber);
    return stats;
  };

  this.getRemoteItems = function *() {
    var sequenceNumber = yield this.getRemoteHistoryLastSequenceNumber();
    var localRepositoryId = yield this.localRepository.getRepositoryId();
    var options = {
      ignoreOriginRepositoryId: localRepositoryId
    };
    if (this.getFilter()) options.filter = this.getFilter();
    var result = yield this.remoteRepository.history.findItemsAfterSequenceNumber(
      sequenceNumber, options
    );
    var remoteRepositoryId = yield this.getRemoteRepositoryId();
    if (result.repositoryId !== remoteRepositoryId) {
      this.emit('remoteRepositoryIdDidChange');
      throw new Error('remote repositoryId did change');
    }
    return result;
  };

  this.saveRemoteItemsInLocalRepository = function *(historyItems) {
    var updatedItemsCount = 0;
    var deletedItemsCount = 0;

    var remoteRepositoryId = yield this.getRemoteRepositoryId();

    var updatedItems = [];
    var deletedItems = [];
    historyItems.forEach(function(item) {
      if (!item.isDeleted) {
        updatedItems.push(item);
      } else {
        deletedItems.push(item);
      }
    });

    // --- Save updated items ---

    var remoteCollection = this.remoteRepository.createRootCollection();
    var ids = _.pluck(updatedItems, 'primaryKey');
    var remoteItems = yield remoteCollection.getItems(ids, { errorIfMissing: false });
    var cache = {};
    var remoteItemsCount = remoteItems.length;
    for (var i = 0; i < remoteItemsCount; i++) {
      var remoteItem = remoteItems[i];
      var className = remoteItem.getClassName();
      var localCollection = this.localRepository.createCollectionFromItemClassName(className, cache);
      localCollection.context = {};
      var localItem = localCollection.unserializeItem(remoteItem);
      localItem.isNew = false;
      yield localItem.save({
        createIfMissing: true,
        originRepositoryId: remoteRepositoryId
      });
      updatedItemsCount++;
      this.emit('didProgress', i / remoteItemsCount);
    }

    // --- Remove deleted items ---

    var localCollection = this.localRepository.createRootCollection();
    localCollection.context = {};
    var ids = _.pluck(deletedItems, 'primaryKey');
    for (var i = 0; i < ids.length; i++) {
      // TODO: implement deleteItems() in kinda-repository and use it there
      var id = ids[i];
      var hasBeenDeleted = yield localCollection.deleteItem(id, {
        errorIfMissing: false,
        originRepositoryId: remoteRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
    }

    return {
      updatedItemsCount: updatedItemsCount,
      deletedItemsCount: deletedItemsCount
    };
  };

  this.sendLocalItems = function *() {
    var result = yield this.getLocalItems();
    var stats = yield this.saveLocalItemsInRemoteRepository(result.items);
    yield this.localRepository.history.deleteItemsUntilSequenceNumber(
      result.lastSequenceNumber
    );
    return stats;
  };

  this.getLocalItems = function *() {
    return yield this.localRepository.history.findItemsAfterSequenceNumber();
  }

  this.saveLocalItemsInRemoteRepository = function *(historyItems) {
    var updatedItemsCount = 0;
    var deletedItemsCount = 0;

    var localRepositoryId = yield this.localRepository.getRepositoryId();

    var updatedItems = [];
    var deletedItems = [];
    historyItems.forEach(function(item) {
      if (!item.isDeleted) {
        updatedItems.push(item);
      } else {
        deletedItems.push(item);
      }
    });

    // --- Save updated items ---

    var localCollection = this.localRepository.createRootCollection();
    localCollection.context = {};
    var ids = _.pluck(updatedItems, 'primaryKey');
    var localItems = yield localCollection.getItems(ids, { errorIfMissing: false });
    var cache = {};
    for (var i = 0; i < localItems.length; i++) {
      var localItem = localItems[i];
      var className = localItem.getClassName();
      var remoteCollection = this.remoteRepository.createCollectionFromItemClassName(className, cache);
      var remoteItem = remoteCollection.unserializeItem(localItem);
      remoteItem.isNew = false;
      yield remoteItem.save({
        createIfMissing: true,
        originRepositoryId: localRepositoryId
      });
      updatedItemsCount++;
    }

    // --- Remove deleted items ---

    var remoteCollection = this.remoteRepository.createRootCollection();
    var ids = _.pluck(deletedItems, 'primaryKey');
    for (var i = 0; i < ids.length; i++) {
      // TODO: implement deleteItems() in kinda-repository and use it there
      var id = ids[i];
      var hasBeenDeleted = yield remoteCollection.deleteItem(id, {
        errorIfMissing: false,
        originRepositoryId: localRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
    }

    return {
      updatedItemsCount: updatedItemsCount,
      deletedItemsCount: deletedItemsCount
    };
  };
});

module.exports = KindaRepositorySynchronizer;
