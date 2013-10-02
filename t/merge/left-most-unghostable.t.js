#!/usr/bin/env node

require('./proof')(4, function (step, Strata,
    tmp, equal, deepEqual, serialize, gather, load, objectify, say) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      equal(cursor.index, 0, 'unghostable');
      cursor.unlock()
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'records');
    strata.balance(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'b', 'c', 'd' ], 'merged');
    objectify(tmp, step());
    load(__dirname + '/fixtures/left-most-unghostable.after.json', step());
  }, function (actual, expected) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'after');
    strata.close(step());
  });
});
