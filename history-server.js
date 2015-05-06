"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');

var HistoryServer = KindaObject.extend('HistoryServer', function() {
  this.plug = function(repositoryServer) {
    this.repositoryServer = repositoryServer;
    repositoryServer.historyServer = this;

    // TODO: don't monkey patch
    var superHandleRequest = repositoryServer._handleRequest;
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
      var result = yield this.repository.history.getLastSequenceNumber();
      ctx.body = result;
    };

    // TODO: don't monkey patch
    repositoryServer.handleFindHistoryItemsAfterSequenceNumberRequest = function *(ctx) {
      yield this.authorizeRequest(ctx, 'findHistoryItemsAfterSequenceNumber');
      var sequenceNumber = ctx.options.startAfter || 0;
      var options = _.omit(ctx.options, ['order', 'startAfter']);
      var result = yield this.repository.history.findItemsAfterSequenceNumber(sequenceNumber, options);
      ctx.body = result;
    };
  };
});

module.exports = HistoryServer;
