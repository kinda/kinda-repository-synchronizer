'use strict';

let _ = require('lodash');
let KindaObject = require('kinda-object');

let HistoryServer = KindaObject.extend('HistoryServer', function() {
  this.plug = function(repositoryServer) {
    this.repositoryServer = repositoryServer;
    repositoryServer.historyServer = this;

    // TODO: don't monkey patch
    let superHandleRequest = repositoryServer._handleRequest;
    repositoryServer._handleRequest = function *(ctx, slug, path, next) {
      if (slug === 'history') {
        yield this.handleGetHistoryLastSequenceNumberRequest(ctx);
        return;
      }

      if (slug === 'history-items') {
        yield this.handleFindHistoryItemsAfterSequenceNumberRequest(ctx);
        return;
      }

      yield superHandleRequest.call(this, ctx, slug, path, next);
    };

    // TODO: don't monkey patch
    repositoryServer.handleGetHistoryLastSequenceNumberRequest = function *(ctx) {
      yield this.authorizeRequest(ctx, 'getHistoryLastSequenceNumber');
      let result = yield this.repository.history.getLastSequenceNumber();
      ctx.body = result;
    };

    // TODO: don't monkey patch
    repositoryServer.handleFindHistoryItemsAfterSequenceNumberRequest = function *(ctx) {
      yield this.authorizeRequest(ctx, 'findHistoryItemsAfterSequenceNumber');
      let sequenceNumber = ctx.options.startAfter || 0;
      let options = _.omit(ctx.options, ['order', 'startAfter']);
      let result = yield this.repository.history.findItemsAfterSequenceNumber(sequenceNumber, options);
      ctx.body = result;
    };
  };
});

module.exports = HistoryServer;
