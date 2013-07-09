#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, equal) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('n', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('b', step());
    }, function (index) {
      cursor.insert('b', 'b', ~index, step());
    }, function (unambiguous) {
      cursor.unlock()
      equal(unambiguous, -1, 'unambiguous');
    }, function () {
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'unchanged');
  }, function() {
    strata.close(step());
  });
});
