#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('b', step());
  }, function (cursor) {
    cursor.remove(cursor.index, step());
  }, function (step, cursor) {
    cursor.next(step());
  }, function (step, cursor) {
    cursor.indexOf('d', step());
  }, function (index, cursor, gather) {
    cursor.remove(index, step());
    cursor.unlock();
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'c' ], 'records');
  }, function () {
    strata.balance(step());
  }, function (load) {
    load(__dirname + '/fixtures/leaf-less-than-max.after.json', step());
  }, function (actual, objectify) {
    objectify(tmp, step());
  }, function (expected, actual, say, gather) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'merge');
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'c' ], 'merged');
    strata.close(step());
  });
});
