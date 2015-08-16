'use strict';

let _ = require('lodash');
let KindaObject = require('kinda-object');

let HistoryServer = KindaObject.extend('HistoryServer', function() {
  this.plug = function(repositoryServer) {
    this.repositoryServer = repositoryServer;
    repositoryServer.historyServer = this;

    // TODO: don't monkey patch
    let superHandleRequest = repositoryServer._handleRequest;
    repositoryServer._handleRequest = async function(ctx, slug, path, next) {
      if (slug === 'history') {
        await this.handleGetHistoryLastSequenceNumberRequest(ctx);
        return;
      }

      if (slug === 'history-items') {
        await this.handleFindHistoryItemsAfterSequenceNumberRequest(ctx);
        return;
      }

      await superHandleRequest.call(this, ctx, slug, path, next);
    };

    // TODO: don't monkey patch
    repositoryServer.handleGetHistoryLastSequenceNumberRequest = async function(ctx) {
      await this.verifyAuthorizationAndAuthorize(ctx, 'getHistoryLastSequenceNumber');
      let result = await this.repository.history.getLastSequenceNumber();
      ctx.body = result;
    };

    // TODO: don't monkey patch
    repositoryServer.handleFindHistoryItemsAfterSequenceNumberRequest = async function(ctx) {
      await this.verifyAuthorizationAndAuthorize(ctx, 'findHistoryItemsAfterSequenceNumber');
      let sequenceNumber = ctx.options.startAfter || 0;
      let options = _.omit(ctx.options, ['order', 'startAfter']);
      let result = await this.repository.history.findItemsAfterSequenceNumber(sequenceNumber, options);
      ctx.body = result;
    };
  };
});

module.exports = HistoryServer;
