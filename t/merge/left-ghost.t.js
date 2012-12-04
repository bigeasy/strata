#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/left-ghost.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('d', async());
  }, function (cursor) {
    cursor.remove(cursor.index, async());
  }, function (async, gather, cursor) {
    cursor.unlock();
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records');
    strata.balance(async());
  }, function (async, gather) {
    gather(async, strata);
  }, function (records, load) {
    deepEqual(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'merged');
    load(__dirname + '/fixtures/left-ghost.after.json', async());
  }, function (actual, objectify) {
    objectify(tmp, async());
  }, function (expected, actual, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'after');
    strata.close(async());
  });
});
