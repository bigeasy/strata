#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('a', async());
  }, function (cursor) {
    async(function () {
      cursor.indexOf('a', async());
    }, function (index) {
      cursor.remove(index, async());
    }, function () {
      cursor.indexOf('b', async());
    }, function (index) {
      cursor.remove(index, async());
      cursor.unlock();
    });
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'c', 'd' ], 'records');
    strata.balance(async());
  }, function (load) {
    load(__dirname + '/fixtures/left-empty.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'merge');
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'c', 'd' ], 'merged');
  }, function() {
    strata.close(async());
  });
});
