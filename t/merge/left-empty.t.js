#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('a', step());
    }, function (index) {
      cursor.remove(index, step());
    }, function () {
      cursor.indexOf('b', step());
    }, function (index) {
      cursor.remove(index, step());
      cursor.unlock();
    });
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'c', 'd' ], 'records');
    strata.balance(step());
  }, function (load) {
    load(__dirname + '/fixtures/left-empty.after.json', step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'merge');
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'c', 'd' ], 'merged');
  }, function() {
    strata.close(step());
  });
});
