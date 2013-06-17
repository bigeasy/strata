#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () { 
    serialize(__dirname + '/fixtures/branch.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('n', step());
  }, function (cursor) {
    step(function () {
      cursor.insert('n', 'n', ~ cursor.index, step());
    }, function () {
      cursor.unlock()
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records');
    strata.balance(step());
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/branch.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'split');

    strata.close(step());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records');
    strata.close(step());
  });
});
