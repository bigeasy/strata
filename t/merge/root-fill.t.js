#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/branch.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('h', async());
  }, function (cursor) {
    async(function () {
      cursor.indexOf('h', async());
    }, function (index) {
      cursor.remove(index, async());
    }, function () {
      cursor.indexOf('i', async());
    }, function (index) {
      cursor.remove(index, async());
      cursor.unlock();
    });
  }, function () {
    strata.mutator('e', async());
  }, function (cursor) {
    async(function () {
      cursor.indexOf('e', async());
    }, function (index) {
      cursor.remove(index, async());
    }, function () {
      cursor.indexOf('g', async());
    }, function (index) {
      cursor.remove(index, async());
      cursor.unlock();
    });
  }, function () {
    strata.mutator('m', async());
  }, function (cursor) {
    async(function () {
      cursor.indexOf('m', async());
    }, function (index) {
      cursor.remove(index, async());
    }, function () {
      cursor.indexOf('n', async());
    }, function (index) {
      cursor.remove(index, async());
      cursor.unlock();
    });
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l' ], 'records');
    strata.balance(async());
  }, function () {
    strata.balance(async());
  }, function (load) {
    load(__dirname + '/fixtures/root-fill.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
//    say(expected);
//    say(actual);

    deepEqual(actual, expected, 'merge');
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'f', 'j', 'k', 'l' ], 'merged');
  }, function() {
    strata.close(async());
  });
});
