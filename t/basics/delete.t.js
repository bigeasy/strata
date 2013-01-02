#!/usr/bin/env node
var fs = require('fs');
require('./proof')(3, function (Strata, async, tmp,  load, objectify, _) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  async(function (serialize) {
    serialize(__dirname + '/fixtures/split.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function (async, gather) {
    gather(async, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "c", "d" ], "records");
  }, function () {
    strata.mutator("a", async());
  }, function (cursor) {
    cursor.indexOf("c", async())
  }, function (i, cursor) {
    cursor.remove(i, async());
  }, function (async, gather, cursor) {
    cursor.unlock()
    gather(async, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "d" ], "records");

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(async());
  });
});
