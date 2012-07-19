#!/usr/bin/env node

require('./proof')(4, function (async, Strata, tmp, deepEqual) {
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
    strata.mutator('g', async());
  }, function (cursor) {
    cursor.remove(cursor.index, async());
  }, function (async, gather, cursor) {
    cursor.unlock()
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'h', 'i', 'l', 'm', 'n' ], 'records after delete');
    strata.mutator('j', async());
  }, function (cursor) {
    cursor.insert('j', 'j', ~cursor.index, async());
  }, function (unambiguous, cursor, ok) {
    ok(unambiguous, 'unambiguous');
    cursor.unlock()
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'h', 'i', 'j', 'l', 'm', 'n' ], 'records after insert');
  }, function() {
    strata.close(async());
  });
});
