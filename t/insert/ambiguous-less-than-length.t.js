#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, ok) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('d', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('e', step());
    }, function (index) {
      cursor.insert('e', 'e', ~index, step());
    }, function (unambiguous) {
      cursor.unlock()
      ok(unambiguous, 'unambiguous');
    }, function () {
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'e', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records after insert');
  }, function() {
    strata.close(step());
  });
});
