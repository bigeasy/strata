#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify, say) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('c', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('c', step());
    }, function (index) {
      cursor.remove(index, step());
    }, function () {
      cursor.indexOf('d', step());
    }, function (index) {
      cursor.remove(index, step());
      cursor.unlock();
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b' ], 'records');

    strata.balance(step());
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/right-empty.after.json', step());
  }, function (actual, expected) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'merge');
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b' ], 'merged');
  }, function() {
    strata.close(step());
  });
});
