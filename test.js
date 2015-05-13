"use strict";

var http = require('http');
require('co-mocha');
var assert = require('chai').assert;
var _ = require('lodash');
var koa = require('koa');
var log = require('kinda-log').create();
var Collection = require('kinda-collection');
var KindaLocalRepository = require('kinda-local-repository');
var KindaRemoteRepository = require('kinda-remote-repository');
var KindaRepositoryServer = require('kinda-repository-server');
var LocalHistory = require('./local-history');
var RemoteHistory = require('./remote-history');
var HistoryServer = require('./history-server');
var RepositorySynchronizer = require('./');

suite('KindaRepositorySynchronizer', function() {
  var frontentLocalRepository, frontentRemoteRepository, backendRepository;
  var frontendPeople, backendPeople;
  var synchronizer, httpServer;

  var catchError = function *(fn) {
    var err;
    try {
      yield fn();
    } catch (e) {
      err = e
    }
    return err;
  };

  suiteSetup(function *() {
    var serverPort = 8888;

    var Elements = Collection.extend('Elements', function() {
      this.Item = this.Item.extend('Element', function() {
        this.addPrimaryKeyProperty('id', String);
        this.addForeignKeyProperty('tenantId', String);
      });
    });

    var People = Elements.extend('People', function() {
      this.Item = this.Item.extend('Person', function() {
        this.addProperty('firstName', String);
        this.addProperty('lastName', String);
        this.addIndex(['lastName', 'firstName']);
      });
    });

    // --- frontend ---

    frontentLocalRepository = KindaLocalRepository.create(
      'FrontendTest',
      'mysql://test@localhost/test',
      [Elements, People]
    );
    frontentLocalRepository.use(LocalHistory.create({ projection: ['tenantId'] }));
    frontendPeople = frontentLocalRepository.createCollection('People');
    frontendPeople.context = {};

    frontentRemoteRepository = KindaRemoteRepository.create(
      'Test',
      'http://localhost:' + serverPort,
      [Elements, People]
    );
    frontentRemoteRepository.use(RemoteHistory.create());

    synchronizer = RepositorySynchronizer.create(
      frontentLocalRepository,
      frontentRemoteRepository,
      { authorizationIsRequired: false }
    );

    // --- backend ---

    backendRepository = KindaLocalRepository.create(
      'BackendTest',
      'mysql://test@localhost/test',
      [Elements, People]
    );
    backendRepository.use(LocalHistory.create({ projection: ['tenantId'] }));
    backendPeople = backendRepository.createCollection('People');
    backendPeople.context = {};

    var repositoryServer = KindaRepositoryServer.create(
      backendRepository, backendRepository
    );

    repositoryServer.use(HistoryServer.create());

    var server = koa();
    server.use(log.getLoggerMiddleware());
    server.use(repositoryServer.getMiddleware());
    httpServer = http.createServer(server.callback());
    httpServer.listen(serverPort);
  });

  suiteTeardown(function *() {
    httpServer.close();
    yield frontentLocalRepository.destroyRepository();
    yield backendRepository.destroyRepository();
  });

  test('test frontend local history', function *() {
    var repositoryId = yield frontentLocalRepository.getRepositoryId();

    var history = frontentLocalRepository.history;

    var result = yield history.getLastSequenceNumber();
    var initialLastSequenceNumber = result.lastSequenceNumber;

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    var person1 = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila',
    });
    yield person1.save();

    var person2 = frontendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel',
    });
    yield person2.save();

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[1].primaryKey, person2.id);

    var result = yield history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    var result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'abcdef' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    var result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { filter: { tenantId: 'ghijkl' } }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    var stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    yield person1.delete();

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.strictEqual(result.items[1].primaryKey, person1.id);
    assert.isTrue(result.items[1].isDeleted);

    var stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 2);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 2);

    yield history.forgetDeletedItems();

    var stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 3);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);

    yield person2.delete();

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 4);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person2.id);
    assert.isTrue(result.items[0].isDeleted);

    var stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 1);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 1);

    yield history.forgetDeletedItems();

    var stats = yield history.getStatistics();
    assert.strictEqual(stats.primaryKeyIndexesCount, 0);
    assert.strictEqual(stats.sequenceNumberIndexesCount, 0);
  });

  test('test frontend remote history', function *() {
    var repositoryId = yield backendRepository.getRepositoryId();
    var remoteRepositoryId = yield frontentRemoteRepository.getRepositoryId();
    assert.ok(remoteRepositoryId);
    assert.strictEqual(remoteRepositoryId, repositoryId);

    var history = frontentRemoteRepository.history;

    var result = yield history.getLastSequenceNumber();
    assert.ok(result.repositoryId);
    assert.strictEqual(result.repositoryId, repositoryId);
    var initialLastSequenceNumber = result.lastSequenceNumber;
    assert.isNumber(initialLastSequenceNumber);

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber);
    assert.strictEqual(result.items.length, 0);

    var person1 = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila',
    });
    yield person1.save();

    var person2 = backendPeople.createItem({
      tenantId: 'ghijkl',
      firstName: 'Jack',
      lastName: 'Daniel',
    });
    yield person2.save({ originRepositoryId: 'a1b2c3' });

    var result = yield history.findItemsAfterSequenceNumber();
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 2);
    assert.strictEqual(result.items[0].primaryKey, person1.id);
    assert.strictEqual(result.items[0].originRepositoryId, repositoryId);
    assert.strictEqual(result.items[1].primaryKey, person2.id);
    assert.strictEqual(result.items[1].originRepositoryId, 'a1b2c3');

    var result = yield history.findItemsAfterSequenceNumber(initialLastSequenceNumber + 2);
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 0);

    var result = yield history.findItemsAfterSequenceNumber(
      initialLastSequenceNumber, { ignoreOriginRepositoryId: 'a1b2c3' }
    );
    assert.strictEqual(result.lastSequenceNumber, initialLastSequenceNumber + 2);
    assert.strictEqual(result.items.length, 1);
    assert.strictEqual(result.items[0].primaryKey, person1.id);

    var result = yield history.findItemsAfterSequenceNumber(
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
    var remoteRepositoryId = yield synchronizer.getRemoteRepositoryId();
    assert.ok(remoteRepositoryId);
    var repositoryId = yield frontentRemoteRepository.getRepositoryId();
    assert.strictEqual(repositoryId, remoteRepositoryId);
    var sequenceNumber = yield synchronizer.getRemoteHistoryLastSequenceNumber();
    assert.strictEqual(sequenceNumber, 0);
  });

  test('synchronize remote changes', function *() {
    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila',
    });
    yield backendPerson.save();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    yield backendPerson.save();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu');

    yield backendPerson.delete();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 1);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var frontendPerson = yield frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);
  });

  test('synchronize local changes', function *() {
    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var frontendPerson = frontendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila',
    });
    yield frontendPerson.save();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var backendPerson = yield backendPeople.getItem(frontendPerson.id);
    assert.strictEqual(backendPerson.firstName, 'Manuel');

    frontendPerson.firstName = 'Manu';
    yield frontendPerson.save();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 1);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield backendPerson.load();
    assert.strictEqual(backendPerson.firstName, 'Manu');

    yield frontendPerson.delete();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 1);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var backendPerson = yield backendPeople.getItem(
      backendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(backendPerson);
  });

  test('synchronize conflictual changes', function *() {
    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var backendPerson = backendPeople.createItem({
      tenantId: 'abcdef',
      firstName: 'Manuel',
      lastName: 'Vila',
    });
    yield backendPerson.save();

    var stats = yield synchronizer.run();

    var frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manuel');

    backendPerson.firstName = 'Manu';
    yield backendPerson.save();

    frontendPerson.firstName = 'Manuelo';
    yield frontendPerson.save();

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manuelo');

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    yield frontendPerson.load();
    assert.strictEqual(frontendPerson.firstName, 'Manu'); // backend always wins

    backendPerson.firstName = 'Manueli';
    yield backendPerson.save();

    yield frontendPerson.delete();

    var frontendPerson = yield frontendPeople.getItem(
      frontendPerson.id, { errorIfMissing: false }
    );
    assert.isUndefined(frontendPerson);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 1);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);

    var frontendPerson = yield frontendPeople.getItem(backendPerson.id);
    assert.strictEqual(frontendPerson.firstName, 'Manueli');

    yield backendPerson.delete();

    yield frontendPerson.delete();

    var stats = yield synchronizer.run();
    assert.strictEqual(stats.updatedLocalItemsCount, 0);
    assert.strictEqual(stats.deletedLocalItemsCount, 0);
    assert.strictEqual(stats.updatedRemoteItemsCount, 0);
    assert.strictEqual(stats.deletedRemoteItemsCount, 0);
  });
});
