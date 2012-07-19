#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('c', async());
  }, function (cursor) {
    cursor.insert('c', 'c', ~cursor.index, async());
  }, function (unambiguous, cursor, ok) {
    cursor.unlock()
    ok(unambiguous, 'unambiguous');
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function() {
    strata.close(async());
  });
});
