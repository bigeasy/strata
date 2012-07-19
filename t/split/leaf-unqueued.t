#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/leaf-remainder.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ "a", "c", "d", "e", "f", "g", "h" ], "records");
    strata.balance(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records, load) {
    deepEqual(records, [ "a", "c", "d", "e", "f", "g", "h" ], "records after balance");

    load(__dirname + '/fixtures/leaf-remainder.before.json', async());
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
