#!/usr/bin/env node

require('./proof')(2, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/between.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function() {
    strata.mutator('b', async());
  }, function (cursor) {
    cursor.insert('b', 'b', ~ cursor.index,  async());
  }, function (async, cursor, load) {
    cursor.unlock();
    load(__dirname + '/fixtures/between.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'insert');
    strata.close(async());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c' ], 'records');
  }, function() {
    strata.close(async());
  });
});
