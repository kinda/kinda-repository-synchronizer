'use strict';

let KindaObject = require('kinda-object');

let RemoteHistory = KindaObject.extend('RemoteHistory', function() {
  this.plug = function(repository) {
    this.repository = repository;
    repository.history = this;
  };

  this.getLastSequenceNumber = async function() {
    let url = this.repository.makeURL('history');
    let params = { method: 'GET', url, json: true };
    this.repository.writeAuthorization(params);
    let res = await this.repository.httpClient.request(params);
    if (res.statusCode !== 200) throw this.repository.createError(res);
    return res.body;
  };

  this.findItemsAfterSequenceNumber = async function(sequenceNumber = 0, options = {}) {
    options.order = ['sequenceNumber'];
    options.startAfter = sequenceNumber;
    let url = this.repository.makeURL('history-items', undefined, undefined, options);
    let params = { method: 'GET', url, json: true, timeout: 60000 };
    this.repository.writeAuthorization(params);
    let res = await this.repository.httpClient.request(params);
    if (res.statusCode !== 200) throw this.repository.createError(res);
    return res.body;
  };
});

module.exports = RemoteHistory;
