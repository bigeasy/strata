#!/usr/bin/env node

require('./proof')(4, function (step, Strata,
    tmp, deepEqual, serialize, gather, ok) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('g', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock()
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'h', 'i', 'l', 'm', 'n' ], 'records after delete');
    strata.mutator('j', step());
  }, function (cursor) {
    step(function () {
      cursor.insert('j', 'j', ~cursor.index, step());
    }, function (unambiguous) {
      ok(unambiguous, 'unambiguous');
      cursor.unlock()
    }, function () {
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'h', 'i', 'j', 'l', 'm', 'n' ], 'records after insert');
  }, function() {
    strata.close(step());
  });
});
