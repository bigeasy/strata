#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    cursor.remove(cursor.index, step());
  }, function (step, cursor, equal) {
    equal(cursor.index, 0, 'unghostable');
    cursor.unlock()
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'records');
    strata.balance(step());
  }, function (gather) {
    gather(step, strata);
  }, function (records, load) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'merged');
    load(__dirname + '/fixtures/left-most-unghostable.after.json', step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'after');
    strata.close(step());
  });
});
