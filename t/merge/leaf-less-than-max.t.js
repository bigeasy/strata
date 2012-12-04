#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('b', async());
  }, function (cursor) {
    cursor.remove(cursor.index, async());
  }, function (async, cursor) {
    cursor.next(async());
  }, function (async, cursor) {
    cursor.indexOf('d', async());
  }, function (index, cursor, gather) {
    cursor.remove(index, async());
    cursor.unlock();
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'c' ], 'records');
  }, function () {
    strata.balance(async());
  }, function (load) {
    load(__dirname + '/fixtures/leaf-less-than-max.after.json', async());
  }, function (actual, objectify) {
    objectify(tmp, async());
  }, function (expected, actual, say, gather) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'merge');
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'c' ], 'merged');
    strata.close(async());
  });
});
