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
    strata.mutator('d', async());
  }, function (cursor) {
    cursor.indexOf('e', async());
  }, function (index, cursor) {
    cursor.insert('e', 'e', ~index, async());
  }, function (unambiguous, cursor, ok) {
    cursor.unlock()
    ok(unambiguous, 'unambiguous');
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'e', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records after insert');
  }, function() {
    strata.close(async());
  });
});
