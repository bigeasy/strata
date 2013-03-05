#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/delete.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd' ], 'records');
  }, function () {
    strata.mutator('c', step());
  }, function (cursor) {
    cursor.indexOf('c', step());
  }, function (i, cursor) {
    cursor.remove(i, step());
  }, function (step, cursor, gather) {
    cursor.unlock()
    gather(step, strata);
  }, function (records, load) {
    deepEqual(records, [ 'a', 'b', 'd' ], 'deleted');
    load(__dirname + '/fixtures/ghost.after.json', step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'directory');

    strata.close(step());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.iterator('a', step());
  }, function (cursor) {
    cursor.next(step())
  }, function (next, cursor, equal) {
    equal(cursor.offset, 1, 'ghosted');
    cursor.unlock();
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd' ], 'reopened');
    strata.close(step());
  });
});
