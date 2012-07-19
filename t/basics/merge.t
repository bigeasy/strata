#!/usr/bin/env node

require("./proof")(3, function (Strata, tmp, load, objectify, serialize, deepEqual, async) {
  var fs = require ('fs'), strata;
  async(function (serialize) {
    serialize(__dirname + "/fixtures/merge.before.json", tmp, async());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(async());
  }, function () {
    strata.mutator("b", async());
  }, function (cursor) {
    async(function () {
      cursor.remove(cursor.index, async());
    }, function () {
      cursor.unlock();
    });
  }, function (records, gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ "a", "c", "d" ], "records");
    strata.balance(async());
  }, function () {
    load(__dirname + '/fixtures/merge.after.json', async());
  }, function (expected) {
    objectify(tmp, async());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);
  
    deepEqual(actual, expected, "merge");
  }, function (records, gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ "a", "c", "d" ], "records");
    strata.balance(async());
  });
});
