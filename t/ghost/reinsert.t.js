#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/reinsert.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('d', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.indexOf('d', step());
    }, function (index) {
      cursor.insert('d', 'd', ~index, step());
    }, function () {
      cursor.unlock();
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'records');
    objectify(tmp, step());
    load(__dirname + '/fixtures/reinsert.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'after tree');
    strata.balance(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'balanced records');
    objectify(tmp, step());
    load(__dirname + '/fixtures/reinsert.balanced.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'balanced tree');
    strata.close(step());
  });
});
