#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/balancer-cached-right.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('e', step());
  }, function (cursor) {
    step(function () {
      cursor.insert('e', 'e', ~ cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [  'b', 'c', 'd',  'e' ], 'records');
    strata.balance(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [  'b', 'c', 'd',  'e' ], 'merged');
  }, function() {
    strata.close(step());
  });
});
