'use strict';

let http = require('http');
require('co-mocha');
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

suite('KindaRepositorySynchronizer', function() {
  let frontentLocalRepository, frontentRemoteRepository, backendRepository;
  let frontendPeople, backendPeople;
  let synchronizer, httpServer;

  suiteSetup(function *() {
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
    yield function(cb) {
      httpServer.listen(serverPort, cb);
    };

    // fix an issue when mocha runs in watch mode:
    yield synchronizer.connectivity.ping();
  });

  suiteTeardown(function *() {
    yield function(cb) {
      httpServer.close(cb);
    };
    yield frontentLocalRepository.destroyRepository();
    yield backendRepository.destroyRepository();
  });

  test('test frontend local history', function *() {
    let repositoryId = yield frontentLocalRepository.getRepositoryId();
    assert.ok(repositoryId);

    let history = frontentLocalRepository.history;

    let result = yield history.getLastSequenceNumber();
    let initialLastSequenceNumber = result.lastSequenceNumber;

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    let person1 = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    yield person1.save();

    let person2 = frontendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel'
    });
    yield person2.save();

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[1].primaryKey, person2.id);

    result = yield history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'abcdef' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'ghijkl' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    let stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    yield person1.delete();

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.strictEqual(result.items[1].primaryKey, person1.id);
    assert.isTrue(result.items[1].isDeleted);

    stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    yield history.forgetDeletedItems();

    stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    yield person2.delete();

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 4);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.isTrue(result.items[0].isDeleted);

    stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    yield history.forgetDeletedItems();

    stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 0);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 0);
  });

  test('test frontend remote history', function *() {
    let repositoryId = yield backendRepository.getRepositoryId();
    let remoteRepositoryId = yield frontentRemoteRepository.getRepositoryId();
    assert.ok(remoteRepositoryId);
    assert.strictEqual(remoteRepositoryId, repositoryId);

    let history = frontentRemoteRepository.history;

    let result = yield history.getLastSequenceNumber();
    assert.ok(result.repositoryId);
    assert.strictEqual(result.repositoryId, repositoryId);
    let initialLastSequenceNumber = result.lastSequenceNumber;
    assert.isNumber(initialLastSequenceNumber);

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    let person1 = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    yield person1.save();

    let person2 = backendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel'
    });
    yield person2.save({ originRepositoryId: 'a1b2c3' });

    result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[0].originRepositoryId, repositoryId);
    assert.strictEqual(result.items[1].primaryKey, person2.id);
    assert.strictEqual(result.items[1].originRepositoryId, 'a1b2c3');

    result = yield history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { ignoreOriginRepositoryId: 'a1b2c3' }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'abcdef' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    yield person1.delete();
    yield person2.delete();
    yield backendRepository.history.forgetDeletedItems();
  });

  test('initialize synchronizer', function *() {
    yield synchronizer.initializeSynchronizer();
    let remoteRepositoryId = yield synchronizer.getRemoteRepositoryId();
    assert.ok(remoteRepositoryId);
    let repositoryId = yield frontentRemoteRepository.getRepositoryId();
    assert.strictEqual(repositoryId, remoteRepositoryId);
    let sequenceNumber = yield synchronizer.getRemoteHistoryLastSequenceNumber();
    assert.strictEqual(sequenceNumber, 0);
  });

  test('synchronize remote changes', function *() {
    let stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    yield backendPerson.save();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    yield backendPerson.save();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu');

    yield backendPerson.delete();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 1);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    frontendPerson = yield frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);
  });

  test('synchronize local changes', function *() {
    let stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let frontendPerson = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    yield frontendPerson.save();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = yield backendPeople.getItem(frontendPerson.id);
    assert.strictEqual(backendPerson.firstName, 'Manuel');

    frontendPerson.firstName = 'Manu';
    yield frontendPerson.save();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield backendPerson.load();
    assert.strictEqual(backendPerson.firstName, 'Manu');

    yield frontendPerson.delete();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 1);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    backendPerson = yield backendPeople.getItem(
      backendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(backendPerson);
  });

  test('synchronize conflictual changes', function *() {
    let stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    let backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila'
    });
    yield backendPerson.save();

    stats = yield synchronizer.run();

    let frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    yield backendPerson.save();

    frontendPerson.firstName = 'Manuelo';
    yield frontendPerson.save();

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manuelo');

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu'); // backend always wins

    backendPerson.firstName = 'Manueli';
    yield backendPerson.save();

    yield frontendPerson.delete();

    frontendPerson = yield frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manueli');

    yield backendPerson.delete();

    yield frontendPerson.delete();

    stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);
  });
});
