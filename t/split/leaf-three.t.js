#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/leaf-three.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('b', step());
  }, function (cursor) {
    step(function () {
      cursor.insert('b', 'b', ~ cursor.index, step());
    }, function () {
      cursor.unlock()
    });
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records');
    strata.balance(step());
  }, function (load) {
    load(__dirname + '/fixtures/leaf-three.after.json', step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'split');
  }, function() {
    strata.close(step());
  });
});
