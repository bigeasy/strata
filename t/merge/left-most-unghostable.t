#!/usr/bin/env node

require('./proof')(4, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('a', async());
  }, function (cursor) {
    cursor.remove(cursor.index, async());
  }, function (async, cursor, equal) {
    equal(cursor.index, 0, 'unghostable');
    cursor.unlock()
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'records');
    strata.balance(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records, load) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'merged');
    load(__dirname + '/fixtures/left-most-unghostable.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'after');
    strata.close(async());
  });
});
