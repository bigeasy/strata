#!/usr/bin/env node

require('./proof')(2, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/leaf-three.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.mutator('b', async());
  }, function (cursor) {
    async(function () {
      cursor.insert('b', 'b', ~ cursor.index, async());
    }, function () {
      cursor.unlock()
    });
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records');
    strata.balance(async());
  }, function (load) {
    load(__dirname + '/fixtures/leaf-three.after.json', async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, 'split');
  }, function() {
    strata.close(async());
  });
});
