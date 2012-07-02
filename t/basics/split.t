#!/usr/bin/env node

require("./proof")(2, function (callback, tmp) {
  var fs = require("fs"), strata, records = [];

  callback(function (serialize) {
    serialize(__dirname + "/fixtures/split.before.json", tmp, callback());
  }, function (Strata) {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(callback());
  }, function () {
    strata.mutator("b", callback("cursor"));
  }, function (cursor) {
    cursor.insert("b", "b", ~ cursor.index, callback());
  }, function (cursor) {
    cursor.unlock();

    records = [];
    strata.iterator("a", callback("cursor"));
  }, function (cursor, gather) {
    gather(cursor, cursor.offset, cursor.length, callback("records"));
  }, function (cursor) {
    cursor.unlock();
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "b", "c", "d" ], "records");
  }, function () {
    strata.balance(callback());
  }, function (load) {
    load(__dirname + "/fixtures/split.after.json", callback("expected"));
  }, function (objectify) {
    objectify(tmp, callback("actual"));
  }, function(actual, expected, say, deepEqual) {
    say(expected);
    say(actual);

    deepEqual(actual, expected, "split");
  });
});
