#!/usr/bin/env node

Error.stackTraceLimit = Infinity;

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function () {
    serialize(__dirname + '/fixtures/branch.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('h', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('h', step());
    }, function (index) {
      cursor.remove(index, step());
    }, function () {
      cursor.indexOf('i', step());
    }, function (index) {
      cursor.remove(index, step());
      cursor.unlock();
    });
  }, function () {
    strata.mutator('e', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('e', step());
    }, function (index) {
      cursor.remove(index, step());
    }, function () {
      cursor.indexOf('g', step());
    }, function (index) {
      cursor.remove(index, step());
      cursor.unlock();
    });
  }, function () {
    strata.mutator('m', step());
  }, function (cursor) {
    step(function () {
      cursor.indexOf('m', step());
    }, function (index) {
      cursor.remove(index, step());
    }, function () {
      cursor.indexOf('n', step());
    }, function (index) {
      cursor.remove(index, step());
      cursor.unlock();
    });
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l' ], 'records');
    strata.balance(step());
  }, function () {
    console.log("second balance");
    strata.balance(step());
  }, function () {
    objectify(tmp, step());
    load(__dirname + '/fixtures/root-fill.after.json', step());
  }, function (actual, expected) {
    deepEqual(actual, expected, 'merge');
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'f', 'j', 'k', 'l' ], 'merged');
  }, function() {
    strata.close(step());
  });
});
