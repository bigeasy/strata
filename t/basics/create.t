#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(4, function (Strata, tmp, load, objectify, callback) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.create(callback());
}, function (callback, equal) {
  equal(strata.stats().size, 4, "json size");
  strata.close(callback());
}, function (callback, ok, tmp, load) {
  ok(1, "created");
  load(__dirname + "/fixtures/create.after.json", callback("expected"));
}, function (callback, tmp, objectify) {
  actual = objectify(tmp, callback("actual"));
  // TODO DRY this up.
}, function (callback, tmp, actual, expected, say, deepEqual, equal, Strata) {
  say(actual);
  say(expected);

  deepEqual(actual, expected, "written");

  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.open(callback());
}, function (callback) {
  strata.iterator("a", callback("cursor"));
}, function (callback, cursor, equal) {
  equal(cursor.length - cursor.offset, 0, "empty");
 // cursor.unlock()
 // strata.close(callback());
});
