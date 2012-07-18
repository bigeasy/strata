#!/usr/bin/env node

require('./proof')(5, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/delete.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd' ], 'records');
  }, function () {
    strata.mutator('c', async());
  }, function (cursor) {
    cursor.indexOf('c', async());
  }, function (i, cursor) {
    cursor.remove(i, async());
  }, function (async, cursor, gather) {
    cursor.unlock()
    gather(async, strata);
  }, function (records, load) {
    deepEqual(records, [ 'a', 'b', 'd' ], 'deleted');
    load(__dirname + '/fixtures/ghost.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'directory');

    strata.close(async());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(async());
  }, function () {
    strata.iterator('a', async());
  }, function (cursor) {
    cursor.next(async())
  }, function (next, cursor, equal) {
    equal(cursor.offset, 1, 'ghosted');
    cursor.unlock();
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd' ], 'reopened');
    strata.close(async());
  });
});
