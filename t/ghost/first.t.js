#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual, serialize, load, objectify) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/first.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/first.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'after');
    strata.vivify(step());
  }, function (result) {
    deepEqual(result, [ { address: -1, children: [ 'b', 'c' ], ghosts: 0 } ], 'ghostbusters');
  }, function () {
    strata.close(step());
  });
});
