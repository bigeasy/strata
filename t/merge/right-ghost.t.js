#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/merge.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.mutator('c', step());
  }, function (cursor) {
    cursor.remove(cursor.index, step());
  }, function (step, gather, cursor) {
    cursor.unlock();
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ "a", "b", "d" ], "records");
    strata.balance(step());
  }, function (step, gather) {
    gather(step, strata);
  }, function (records, load) {
    deepEqual(records, [ 'a', 'b', 'd' ], 'merged');
    load(__dirname + '/fixtures/right-ghost.after.json', step());
  }, function (actual, objectify) {
    objectify(tmp, step());
  }, function (expected, actual, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'after');
    strata.close(step());
  });
});
