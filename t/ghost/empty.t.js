#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, deepEqual, serialize, load, objectify) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/empty.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('c', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    strata.mutator('f', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    strata.mutator('i', step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function () {
    strata.balance(step());
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/empty.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'after balance');
    strata.close(step());
  });
});
