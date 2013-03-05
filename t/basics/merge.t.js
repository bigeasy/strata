#!/usr/bin/env node

require("./proof")(3, function (Strata, tmp, load, objectify, serialize, deepEqual, step) {
  var fs = require ('fs'), strata;
  step(function (serialize) {
    serialize(__dirname + "/fixtures/merge.before.json", tmp, step());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.mutator("b", step());
  }, function (cursor) {
    step(function () {
      cursor.remove(cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  }, function (records, gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ "a", "c", "d" ], "records");
    strata.balance(step());
  }, function () {
    load(__dirname + '/fixtures/merge.after.json', step());
  }, function (expected) {
    objectify(tmp, step());
  }, function (actual, expected, say) {
    say(expected);
    say(actual);
  
    deepEqual(actual, expected, "merge");
  }, function (records, gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ "a", "c", "d" ], "records");
    strata.balance(step());
  });
});
