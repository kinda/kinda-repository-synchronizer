"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');
var httpClient = require('kinda-http-client').create();

var RemoteHistory = KindaObject.extend('RemoteHistory', function() {
  this.plug = function(repository) {
    this.repository = repository;
    repository.history = this;
  };

  this.getLastSequenceNumber = function *() {
    var url = this.repository.makeURL('history');
    var params = { method: 'GET', url: url };
    this.repository.writeAuthorization(params);
    var res = yield httpClient.request(params);
    if (res.statusCode !== 200) throw this.repository.createError(res);
    return res.body;
  };

  this.findItemsAfterSequenceNumber = function *(sequenceNumber, options) {
    if (sequenceNumber == null) sequenceNumber = 0;
    if (!options) options = {};
    options.order = ['sequenceNumber'];
    options.startAfter = sequenceNumber;
    var url = this.repository.makeURL('history-items', undefined, undefined, options);
    var params = { method: 'GET', url: url };
    this.repository.writeAuthorization(params);
    var res = yield httpClient.request(params);
    if (res.statusCode !== 200) throw this.repository.createError(res);
    return res.body;
  };
});

module.exports = RemoteHistory;
