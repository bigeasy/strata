#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/between.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function() {
    strata.mutator('b', step());
  }, function (cursor) {
    cursor.insert('b', 'b', ~ cursor.index,  step());
  }, function (step, cursor, load) {
    cursor.unlock();
    load(__dirname + '/fixtures/between.after.json', step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'insert');
    strata.close(step());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c' ], 'records');
  }, function() {
    strata.close(step());
  });
});
