#!/usr/bin/env node

require("./proof")(3, function (step, tmp) {
  var fs = require("fs"), strata, records = [];

  step(function (serialize) {
    serialize(__dirname + "/fixtures/split.before.json", tmp, step());
  }, function (Strata) {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.mutator("b", step());
  }, function (cursor) {
    cursor.insert("b", "b", ~ cursor.index, step());
  }, function ($1, cursor, gather) {
    cursor.unlock();
    gather(step, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "b", "c", "d" ], "records");
  }, function () {
    strata.balance(step());
  }, function (load) {
    load(__dirname + "/fixtures/split.after.json", step());
  }, function (expected, objectify) {
    objectify(tmp, step());
  }, function(actual, expected, say, deepEqual) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, "split");

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(step());
  });
});
