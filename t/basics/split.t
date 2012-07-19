#!/usr/bin/env node

require("./proof")(2, function (async, tmp) {
  var fs = require("fs"), strata, records = [];

  async(function (serialize) {
    serialize(__dirname + "/fixtures/split.before.json", tmp, async());
  }, function (Strata) {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(async());
  }, function () {
    strata.mutator("b", async());
  }, function (cursor) {
    cursor.insert("b", "b", ~ cursor.index, async());
  }, function ($1, cursor, gather) {
    cursor.unlock();
    gather(async, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "b", "c", "d" ], "records");
  }, function () {
    strata.balance(async());
  }, function (load) {
    load(__dirname + "/fixtures/split.after.json", async());
  }, function (expected, objectify) {
    objectify(tmp, async());
  }, function(actual, expected, say, deepEqual) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, "split");
  });
});
