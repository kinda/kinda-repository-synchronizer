'use strict';

let http = require('http');
let assert = require('chai').assert;
let koa = require('koa');
let log = require('kinda-log').create();
let Collection = require('kinda-collection');
let KindaLocalRepository = require('kinda-local-repository');
let KindaRemoteRepository = require('kinda-remote-repository');
let KindaRepositoryServer = require('kinda-repository-server');
let KindaRepositorySynchronizer = require('./src');
let LocalHistory = KindaRepositorySynchronizer.LocalHistory;
let RemoteHistory = KindaRepositorySynchronizer.RemoteHistory;
let HistoryServer = KindaRepositorySynchronizer.HistoryServer;

suite.skip('KindaRepositorySynchronizer', function() {
  let frontentLocalRepository, frontentRemoteRepository, backendRepository;
  let frontendPeople, backendPeople;
  let synchronizer, httpServer;

  suiteSetup(async function() {
    let serverPort = 8888;

    let Elements = Collection.extend('Elements', function() {
      this.Item = this.Item.extend('Element', function() {
        this.addPrimaryKeyProperty('id', String);
        this.addForeignKeyProperty('tenantId', String);
      });
    });

    let People = Elements.extend('People', function() {
      this.Item = this.Item.extend('Person', function() {
        this.addProperty('firstName', String);
        this.addProperty('lastName', String);
        this.addIndex(['lastName', 'firstName']);
      });
    });

    // --- frontend ---

    frontentLocalRepository = KindaLocalRepository.create({
      name: 'FrontendTest',
      url: 'mysql://test@localhost/test',
      collections: [Elements, People]
    });
    frontentLocalRepository.use(LocalHistory.create({ projection: ['tenantId'] }));
    frontendPeople = frontentLocalRepository.createCollection('People');

    frontentRemoteRepository = KindaRemoteRepository.create({
      name: 'Test',
      url: 'http://localhost:' + serverPort,
      collections: [Elements, People]
    });
    frontentRemoteRepository.use(RemoteHistory.create());

    synchronizer = KindaRepositorySynchronizer.create({
      localRepository: frontentLocalRepository,
      remoteRepository: frontentRemoteRepository,
      authorizationIsRequired: false
    });

    // --- backend ---

    backendRepository = KindaLocalRepository.create({
      name: 'BackendTest',
      url: 'mysql://test@localhost/test',
      collections: [Elements, People]
    });
    backendRepository.use(LocalHistory.create({ projection: ['tenantId'] }));
    backendPeople = backendRepository.createCollection('People');

    let repositoryServer = KindaRepositoryServer.create({
      repository: backendRepository
    });

    repositoryServer.use(HistoryServer.create());

    let server = koa();
    server.use(log.getLoggerMiddleware());
    server.use(repositoryServer.getMiddleware());
    httpServer = http.createServer(server.callback());
    await new Promise((resolve, reject) => {
      httpServer.listen(serverPort, function(err, res) {
        if (err) reject(err); else resolve(res);
      });
    });

    // fix an issue when mocha runs in watch mode:
    await synchronizer.connectivity.ping();
  });

  suiteTeardown(async function() {
    await new Promise((resolve, reject) => {
      httpServer.close(function(err, res) {
        if (err) reject(err); else resolve(res);
      });
    });
    await frontentLocalRepository.destroyRepository();
    await backendRepository.destroyRepository();
  });

  test('test frontend local history', async function() {
    let repositoryId = await frontentLocalRepository.getRepositoryId();
    assert.ok(repositoryId);

    let history = frontentLocalRepository.history;

    let result = await history.getLastSequenceNumber();
    let initialLastSequenceNumber = result.lastSequenceNumber;

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    let person1 = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    await person1.save();

    let person2 = frontendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel'
    });
    await person2.save();

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[1].primaryKey, person2.id);

    result = await history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    result = await history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'abcdef' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    result = await history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'ghijkl' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    let stats = await history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    await person1.delete();

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.strictEqual(result.items[1].primaryKey, person1.id);
    assert.isTrue(result.items[1].isDeleted);

    stats = await history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    await history.forgetDeletedItems();

    stats = await history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    await person2.delete();

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 4);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.isTrue(result.items[0].isDeleted);

    stats = await history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    await history.forgetDeletedItems();

    stats = await history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 0);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 0);
  });

  test('test frontend remote history', async function() {
    let repositoryId = await backendRepository.getRepositoryId();
    let remoteRepositoryId = await frontentRemoteRepository.getRepositoryId();
    assert.ok(remoteRepositoryId);
    assert.strictEqual(remoteRepositoryId, repositoryId);

    let history = frontentRemoteRepository.history;

    let result = await history.getLastSequenceNumber();
    assert.ok(result.repositoryId);
    assert.strictEqual(result.repositoryId, repositoryId);
    let initialLastSequenceNumber = result.lastSequenceNumber;
    assert.isNumber(initialLastSequenceNumber);

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    let person1 = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    await person1.save();

    let person2 = backendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel'
    });
    await person2.save({ originRepositoryId: 'a1b2c3' });

    result = await history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[0].originRepositoryId, repositoryId);
    assert.strictEqual(result.items[1].primaryKey, person2.id);
    assert.strictEqual(result.items[1].originRepositoryId, 'a1b2c3');

    result = await history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    result = await history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { ignoreOriginRepositoryId: 'a1b2c3' }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    result = await history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'abcdef' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    await person1.delete();
    await person2.delete();
    await backendRepository.history.forgetDeletedItems();
  });

  test('initialize synchronizer', async function() {
    await synchronizer.initializeSynchronizer();
    let remoteRepositoryId = await synchronizer.getRemoteRepositoryId();
    assert.ok(remoteRepositoryId);
    let repositoryId = await frontentRemoteRepository.getRepositoryId();
    assert.strictEqual(repositoryId, remoteRepositoryId);
    let sequenceNumber = await synchronizer.getRemoteHistoryLastSequenceNumber();
    assert.strictEqual(sequenceNumber, 0);
  });

  test('synchronize remote changes', async function() {
    let stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    await backendPerson.save();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let frontendPerson = await frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    await backendPerson.save();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    await frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu');

    await backendPerson.delete();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 1);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    frontendPerson = await frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);
  });

  test('synchronize local changes', async function() {
    let stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let frontendPerson = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    await frontendPerson.save();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = await backendPeople.getItem(frontendPerson.id);
    assert.strictEqual(backendPerson.firstName, 'Manuel');

    frontendPerson.firstName = 'Manu';
    await frontendPerson.save();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    await backendPerson.load();
    assert.strictEqual(backendPerson.firstName, 'Manu');

    await frontendPerson.delete();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 1);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    backendPerson = await backendPeople.getItem(
      backendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(backendPerson);
  });

  test('synchronize conflictual changes', async function() {
    let stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    await backendPerson.save();

    stats = await synchronizer.run();

    let frontendPerson = await frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    await backendPerson.save();

    frontendPerson.firstName = 'Manuelo';
    await frontendPerson.save();

    await frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manuelo');

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    await frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu'); // backend always wins

    backendPerson.firstName = 'Manueli';
    await backendPerson.save();

    await frontendPerson.delete();

    frontendPerson = await frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    frontendPerson = await frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manueli');

    await backendPerson.delete();

    await frontendPerson.delete();

    stats = await synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);
  });
});
