'use strict';

let _ = require('lodash');
let co = require('co');
let wait = require('co-wait');
let KindaObject = require('kinda-object');
let KindaEventManager = require('kinda-event-manager');
let util = require('kinda-util').create();
let KindaLog = require('kinda-log');
let KindaConnectivity = require('kinda-connectivity');
let LocalHistory = require('./local-history');
let RemoteHistory = require('./remote-history');
let HistoryServer = require('./history-server');

let KindaRepositorySynchronizer = KindaObject.extend('KindaRepositorySynchronizer', function() {
  this.include(KindaEventManager);

  this.creator = function(options = {}) {
    if (!options.localRepository) throw new Error('local repository is missing');
    let localRepository = options.localRepository;
    if (!options.remoteRepository) throw new Error('remote repository is missing');
    let remoteRepository = options.remoteRepository;

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

    if (options.filter) this.filter = options.filter;

    this.filterIsRequired = options.filterIsRequired;

    this.authorizationIsRequired = (
      options.authorizationIsRequired != null ?
      options.authorizationIsRequired : true
    );

    let log = options.log;
    if (log) {
      if (!KindaLog.isClassOf(log)) log = KindaLog.create(log);
    } else {
      log = this.localRepository.log;
    }
    this.log = log;

    let connectivity = options.connectivity;
    if (!KindaConnectivity.isClassOf(connectivity)) {
      if (!connectivity) connectivity = {};
      if (!connectivity.url) {
        let url = remoteRepository.baseURL;
        if (!_.endsWith(url, '/')) url += '/';
        url += 'ping';
        connectivity.url = url;
      }
      connectivity = KindaConnectivity.create(connectivity);
    }
    this.connectivity = connectivity;
    this.connectivity.monitor();

    localRepository.onAsync('willDestroy', function *() {
      yield this.suspend();
    }.bind(this));

    localRepository.onAsync('didDestroy', function *() {
      this.hasBeenInitialized = false;
      delete this._remoteRepositoryId;
      delete this._remoteHistoryLastSequenceNumber;
      yield this.resume();
    }.bind(this));
  };

  Object.defineProperty(this, 'filter', {
    get() {
      return this._filter;
    },
    set(filter) {
      this._filter = filter;
    }
  });

  this.getRemoteRepositoryId = function *() {
    if (this._remoteRepositoryId) return this._remoteRepositoryId;
    let record = yield this.localRepository.loadRepositoryRecord();
    let remoteRepositoryId = record.remoteRepositoryId;
    if (!remoteRepositoryId) return undefined;
    this._remoteRepositoryId = remoteRepositoryId;
    return remoteRepositoryId;
  };

  this.setRemoteRepositoryId = function *(remoteRepositoryId) {
    let record = yield this.localRepository.loadRepositoryRecord();
    record.remoteRepositoryId = remoteRepositoryId;
    yield this.localRepository.saveRepositoryRecord(record);
    this._remoteRepositoryId = remoteRepositoryId;
  };

  this.getRemoteHistoryLastSequenceNumber = function *() {
    if (this._remoteHistoryLastSequenceNumber != null) {
      return this._remoteHistoryLastSequenceNumber;
    }
    let record = yield this.localRepository.loadRepositoryRecord();
    let sequenceNumber = record.remoteHistoryLastSequenceNumber;
    if (sequenceNumber == null) return undefined;
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
    return sequenceNumber;
  };

  this.setRemoteHistoryLastSequenceNumber = function *(sequenceNumber) {
    let record = yield this.localRepository.loadRepositoryRecord();
    record.remoteHistoryLastSequenceNumber = sequenceNumber;
    yield this.localRepository.saveRepositoryRecord(record);
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
  };

  this.initializeSynchronizer = function *() {
    if (this.hasBeenInitialized) return;
    let repositoryId = yield this.getRemoteRepositoryId();
    if (!repositoryId) {
      repositoryId = yield this.remoteRepository.getRepositoryId();
      yield this.setRemoteRepositoryId(repositoryId);
    }
    let sequenceNumber = yield this.getRemoteHistoryLastSequenceNumber();
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
            this.log.error(err);
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
      this.log.error(err.stack || err);
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

  Object.defineProperty(this, 'isStarted', {
    get() {
      return this._isStarted;
    }
  });

  Object.defineProperty(this, 'lastSynchronizationDate', {
    get() {
      return this._lastSynchronizationDate;
    }
  });

  this.run = function *(quietMode) {
    let stats = {};
    if (this._isRunning) return stats;
    if (this._isSuspended) return stats;
    if (this.authorizationIsRequired && !this.remoteRepository.getAuthorization()) {
      if (!quietMode) {
        this.log.notice('an authorization is required to run the synchronizer');
      }
      return stats;
    }
    if (this.filterIsRequired && !this.filter) {
      if (!quietMode) {
        this.log.notice('a filter is required to run the synchronizer');
      }
      return stats;
    }
    if (this.connectivity.isOffline == null) yield this.connectivity.ping();
    if (this.connectivity.isOffline) {
      if (!quietMode) {
        this.log.notice('a working connection is required to run the synchronizer');
      }
      return stats;
    }
    try {
      this._isRunning = true;
      let remoteRepositoryId = yield this.getRemoteRepositoryId();
      let isFirstSynchronization = !remoteRepositoryId;
      this.emit('willRun', isFirstSynchronization);
      yield this.initializeSynchronizer();
      let localStats = yield this.receiveRemoteItems();
      let remoteStats = yield this.sendLocalItems();
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
    let result = yield this.getRemoteItems();
    let stats = yield this.saveRemoteItemsInLocalRepository(result.items);
    yield this.setRemoteHistoryLastSequenceNumber(result.lastSequenceNumber);
    return stats;
  };

  this.getRemoteItems = function *() {
    let sequenceNumber = yield this.getRemoteHistoryLastSequenceNumber();
    let localRepositoryId = yield this.localRepository.getRepositoryId();
    let options = {
      ignoreOriginRepositoryId: localRepositoryId
    };
    if (this.filter) options.filter = this.filter;
    let result = yield this.remoteRepository.history.findItemsAfterSequenceNumber(
      sequenceNumber, options
    );
    let remoteRepositoryId = yield this.getRemoteRepositoryId();
    if (result.repositoryId !== remoteRepositoryId) {
      this.emit('remoteRepositoryIdDidChange');
      throw new Error('remote repositoryId did change');
    }
    return result;
  };

  this.saveRemoteItemsInLocalRepository = function *(historyItems) {
    let updatedItemsCount = 0;
    let deletedItemsCount = 0;

    let remoteRepositoryId = yield this.getRemoteRepositoryId();

    let updatedItems = [];
    let deletedItems = [];
    for (let item of historyItems) {
      if (!item.isDeleted) {
        updatedItems.push(item);
      } else {
        deletedItems.push(item);
      }
    }

    // --- Save updated items ---

    let rootRemoteCollection = this.remoteRepository.createRootCollection();
    let ids = _.pluck(updatedItems, 'primaryKey');
    let remoteItems = yield rootRemoteCollection.getItems(ids, { errorIfMissing: false });
    let cache = {};
    let remoteItemsCount = remoteItems.length;
    for (let i = 0; i < remoteItemsCount; i++) {
      let remoteItem = remoteItems[i];
      let className = remoteItem.class.name;
      let localCollection = this.localRepository.createCollectionFromItemClassName(className, cache);
      let localItem = localCollection.unserializeItem(remoteItem);
      localItem.isNew = false;
      yield localItem.save({
        createIfMissing: true,
        source: 'localSynchronizer',
        originRepositoryId: remoteRepositoryId
      });
      updatedItemsCount++;
    }

    // --- Remove deleted items ---

    let rootLocalCollection = this.localRepository.createRootCollection();
    ids = _.pluck(deletedItems, 'primaryKey');
    for (let id of ids) {
      // TODO: implement deleteItems() in kinda-repository and use it there
      let localItem = yield rootLocalCollection.getItem(id, { errorIfMissing: false });
      if (!localItem) continue;
      let hasBeenDeleted = yield localItem.delete({
        source: 'localSynchronizer',
        originRepositoryId: remoteRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
    }

    return { updatedItemsCount, deletedItemsCount };
  };

  this.sendLocalItems = function *() {
    let result = yield this.getLocalItems();
    let stats = yield this.saveLocalItemsInRemoteRepository(result.items);
    yield this.localRepository.history.deleteItemsUntilSequenceNumber(
      result.lastSequenceNumber
    );
    return stats;
  };

  this.getLocalItems = function *() {
    this.emit('didProgress', { task: 'loadingLocalHistory' });
    return yield this.localRepository.history.findItemsAfterSequenceNumber();
  };

  this.saveLocalItemsInRemoteRepository = function *(historyItems) {
    let updatedItemsCount = 0;
    let deletedItemsCount = 0;

    let localRepositoryId = yield this.localRepository.getRepositoryId();

    let updatedItems = [];
    let deletedItems = [];
    for (let item of historyItems) {
      if (!item.isDeleted) {
        updatedItems.push(item);
      } else {
        deletedItems.push(item);
      }
    }

    // --- Save updated items ---

    let rootLocalCollection = this.localRepository.createRootCollection();
    let ids = _.pluck(updatedItems, 'primaryKey');
    let localItems = yield rootLocalCollection.getItems(ids, { errorIfMissing: false });
    let cache = {};
    for (let localItem of localItems) {
      let className = localItem.class.name;
      let remoteCollection = this.remoteRepository.createCollectionFromItemClassName(className, cache);
      let remoteItem = remoteCollection.unserializeItem(localItem);
      remoteItem.isNew = false;
      yield remoteItem.save({
        createIfMissing: true,
        source: 'remoteSynchronizer',
        originRepositoryId: localRepositoryId
      });
      updatedItemsCount++;
    }

    // --- Remove deleted items ---

    let rootRemoteCollection = this.remoteRepository.createRootCollection();
    ids = _.pluck(deletedItems, 'primaryKey');
    for (let id of ids) {
      // TODO: implement deleteItems() in kinda-repository and use it there
      let hasBeenDeleted = yield rootRemoteCollection.deleteItem(id, {
        errorIfMissing: false,
        source: 'remoteSynchronizer',
        originRepositoryId: localRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
    }

    return { updatedItemsCount, deletedItemsCount };
  };
});

KindaRepositorySynchronizer.LocalHistory = LocalHistory;
KindaRepositorySynchronizer.RemoteHistory = RemoteHistory;
KindaRepositorySynchronizer.HistoryServer = HistoryServer;

module.exports = KindaRepositorySynchronizer;
