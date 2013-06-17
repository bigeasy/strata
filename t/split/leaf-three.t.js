#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/leaf-three.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('b', step());
  }, function (cursor) {
    step(function () {
      cursor.insert('b', 'b', ~ cursor.index, step());
    }, function () {
      cursor.unlock()
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records');
    strata.balance(step());
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/leaf-three.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'split');
  }, function() {
    strata.close(step());
  });
});
