'use strict';

let _ = require('lodash');
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

    localRepository.on('willDestroy', async function() {
      await this.suspend();
    }.bind(this));

    localRepository.on('didDestroy', async function() {
      this.hasBeenInitialized = false;
      delete this._remoteRepositoryId;
      delete this._remoteHistoryLastSequenceNumber;
      await this.resume();
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

  Object.defineProperty(this, 'throttlingTime', {
    get() {
      return this._throttlingTime;
    },
    set(throttlingTime) {
      this._throttlingTime = throttlingTime;
    }
  });

  this.getRemoteRepositoryId = async function() {
    if (this._remoteRepositoryId) return this._remoteRepositoryId;
    let record = await this.localRepository.loadRepositoryRecord();
    let remoteRepositoryId = record.remoteRepositoryId;
    if (!remoteRepositoryId) return undefined;
    this._remoteRepositoryId = remoteRepositoryId;
    return remoteRepositoryId;
  };

  this.setRemoteRepositoryId = async function(remoteRepositoryId) {
    await this.localRepository.transaction(async function(repository) {
      let record = await repository.loadRepositoryRecord();
      record.remoteRepositoryId = remoteRepositoryId;
      await repository.saveRepositoryRecord(record);
    });
    this._remoteRepositoryId = remoteRepositoryId;
  };

  this.getRemoteHistoryLastSequenceNumber = async function() {
    if (this._remoteHistoryLastSequenceNumber != null) {
      return this._remoteHistoryLastSequenceNumber;
    }
    let record = await this.localRepository.loadRepositoryRecord();
    let sequenceNumber = record.remoteHistoryLastSequenceNumber;
    if (sequenceNumber == null) return undefined;
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
    return sequenceNumber;
  };

  this.setRemoteHistoryLastSequenceNumber = async function(sequenceNumber) {
    if (this._remoteHistoryLastSequenceNumber === sequenceNumber) return;
    await this.localRepository.transaction(async function(repository) {
      let record = await repository.loadRepositoryRecord();
      record.remoteHistoryLastSequenceNumber = sequenceNumber;
      await repository.saveRepositoryRecord(record);
    });
    this._remoteHistoryLastSequenceNumber = sequenceNumber;
  };

  this.initializeSynchronizer = async function() {
    if (this.hasBeenInitialized) return;
    let repositoryId = await this.getRemoteRepositoryId();
    if (!repositoryId) {
      repositoryId = await this.remoteRepository.getRepositoryId();
      await this.setRemoteRepositoryId(repositoryId);
    }
    let sequenceNumber = await this.getRemoteHistoryLastSequenceNumber();
    if (sequenceNumber == null) {
      sequenceNumber = 0;
      await this.setRemoteHistoryLastSequenceNumber(sequenceNumber);
    }
    this.hasBeenInitialized = true;
    await this.emit('didInitialize');
  };

  this.start = function() {
    (async function() {
      if (this._isStarted) return;
      this._isStarted = true;
      this._isStopping = false;
      try {
        this.emit('didStart');
        while (!this._isStopping) {
          try {
            await this.run(true);
          } catch (err) {
            this.log.error(err);
          }
          if (!this._isStopping) {
            this._timeout = util.createTimeout(30 * 1000); // 30 seconds
            await this._timeout.start();
            this._timeout = undefined;
          }
        }
      } finally {
        this._isStarted = false;
        this._isStopping = false;
      }
      this.emit('didStop');
    }).call(this).catch(err => {
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

  this.waitStop = async function() {
    while (this._isStarted || this._isRunning) {
      await util.timeout(100);
    }
  };

  this.suspend = async function() {
    this._isSuspended = true;
    while (this._isRunning) {
      await util.timeout(100);
    }
  };

  this.resume = async function() {
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

  this.run = async function(quietMode) {
    let stats = {};
    if (this._isRunning) return stats;
    if (this._isSuspended) return stats;
    if (this.authorizationIsRequired && !this.remoteRepository.authorization) {
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
    if (this.connectivity.isOffline == null) await this.connectivity.ping();
    if (this.connectivity.isOffline) {
      if (!quietMode) {
        this.log.notice('a working connection is required to run the synchronizer');
      }
      return stats;
    }
    try {
      this._isRunning = true;
      let remoteRepositoryId = await this.getRemoteRepositoryId();
      let info = { isFirstSynchronization: !remoteRepositoryId };
      this.emit('willRun', info);
      await this.initializeSynchronizer();
      let localStats = await this.receiveRemoteItems();
      let remoteStats = await this.sendLocalItems();
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

  this.receiveRemoteItems = async function() {
    let result = await this.getRemoteItems();
    let stats = await this.saveRemoteItemsInLocalRepository(result.items);
    await this.setRemoteHistoryLastSequenceNumber(result.lastSequenceNumber);
    return stats;
  };

  this.getRemoteItems = async function() {
    let sequenceNumber = await this.getRemoteHistoryLastSequenceNumber();
    let localRepositoryId = await this.localRepository.getRepositoryId();
    let options = {
      ignoreOriginRepositoryId: localRepositoryId
    };
    if (this.filter) options.filter = this.filter;
    this.emit('didProgress', { task: 'receivingRemoteHistory' });
    let result = await this.remoteRepository.history.findItemsAfterSequenceNumber(
      sequenceNumber, options
    );
    let remoteRepositoryId = await this.getRemoteRepositoryId();
    if (result.repositoryId !== remoteRepositoryId) {
      this.emit('remoteRepositoryIdDidChange');
      throw new Error('remote repositoryId did change');
    }
    return result;
  };

  this.saveRemoteItemsInLocalRepository = async function(historyItems) {
    let updatedItemsCount = 0;
    let deletedItemsCount = 0;

    let remoteRepositoryId = await this.getRemoteRepositoryId();

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
    this.emit('didProgress', { task: 'receivingRemoteItems' });
    let remoteItems = await rootRemoteCollection.getItems(ids, { errorIfMissing: false });
    let cache = {};
    let progressCount = 0;
    let progressTotal = remoteItems.length;
    for (let remoteItem of remoteItems) {
      if (this.throttlingTime) await util.timeout(this.throttlingTime);
      this.emit('didProgress', {
        task: 'savingItemsInLocalRepository',
        progress: progressCount / progressTotal
      });
      let className = remoteItem.class.name;
      let localCollection = this.localRepository.createCollectionFromItemClassName(className, cache);
      let localItem = localCollection.unserializeItem(remoteItem);
      localItem.isNew = false;
      await localItem.save({
        createIfMissing: true,
        source: 'localSynchronizer',
        originRepositoryId: remoteRepositoryId
      });
      updatedItemsCount++;
      progressCount++;
    }

    // --- Remove deleted items ---

    let rootLocalCollection = this.localRepository.createRootCollection();
    ids = _.pluck(deletedItems, 'primaryKey');
    progressCount = 0;
    progressTotal = ids.length;
    for (let id of ids) {
      if (this.throttlingTime) await util.timeout(this.throttlingTime);
      // TODO: implement deleteItems() in kinda-repository and use it there
      this.emit('didProgress', {
        task: 'deletingItemsInLocalRepository',
        progress: progressCount / progressTotal
      });
      let localItem = await rootLocalCollection.getItem(id, { errorIfMissing: false });
      if (!localItem) continue;
      let hasBeenDeleted = await localItem.delete({
        source: 'localSynchronizer',
        originRepositoryId: remoteRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
      progressCount++;
    }

    return { updatedItemsCount, deletedItemsCount };
  };

  this.sendLocalItems = async function() {
    let result = await this.getLocalItems();
    let stats = await this.saveLocalItemsInRemoteRepository(result.items);
    await this.localRepository.history.deleteItemsUntilSequenceNumber(
      result.lastSequenceNumber
    );
    return stats;
  };

  this.getLocalItems = async function() {
    this.emit('didProgress', { task: 'loadingLocalHistory' });
    return await this.localRepository.history.findItemsAfterSequenceNumber();
  };

  this.saveLocalItemsInRemoteRepository = async function(historyItems) {
    let updatedItemsCount = 0;
    let deletedItemsCount = 0;

    let localRepositoryId = await this.localRepository.getRepositoryId();

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
    this.emit('didProgress', { task: 'loadingLocalItems' });
    let localItems = await rootLocalCollection.getItems(ids, { errorIfMissing: false });
    let cache = {};
    let progressCount = 0;
    let progressTotal = localItems.length;
    for (let localItem of localItems) {
      if (this.throttlingTime) await util.timeout(this.throttlingTime);
      this.emit('didProgress', {
        task: 'savingItemsInRemoteRepository',
        progress: progressCount / progressTotal
      });
      let className = localItem.class.name;
      let remoteCollection = this.remoteRepository.createCollectionFromItemClassName(className, cache);
      let remoteItem = remoteCollection.unserializeItem(localItem);
      remoteItem.isNew = false;
      await remoteItem.save({
        createIfMissing: true,
        source: 'remoteSynchronizer',
        originRepositoryId: localRepositoryId
      });
      updatedItemsCount++;
      progressCount++;
    }

    // --- Remove deleted items ---

    let rootRemoteCollection = this.remoteRepository.createRootCollection();
    ids = _.pluck(deletedItems, 'primaryKey');
    progressCount = 0;
    progressTotal = ids.length;
    for (let id of ids) {
      if (this.throttlingTime) await util.timeout(this.throttlingTime);
      // TODO: implement deleteItems() in kinda-repository and use it there
      this.emit('didProgress', {
        task: 'deletingItemsInRemoteRepository',
        progress: progressCount / progressTotal
      });
      let hasBeenDeleted = await rootRemoteCollection.deleteItem(id, {
        errorIfMissing: false,
        source: 'remoteSynchronizer',
        originRepositoryId: localRepositoryId
      });
      if (hasBeenDeleted) deletedItemsCount++;
      progressCount++;
    }

    return { updatedItemsCount, deletedItemsCount };
  };
});

KindaRepositorySynchronizer.LocalHistory = LocalHistory;
KindaRepositorySynchronizer.RemoteHistory = RemoteHistory;
KindaRepositorySynchronizer.HistoryServer = HistoryServer;

module.exports = KindaRepositorySynchronizer;
