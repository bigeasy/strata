#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(4,
function (Strata, equal, deepEqual, say, tmp, callback) {
  callback(function () {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(callback());

  }, function () {

    equal(strata.stats().size, 4, "json size");
    strata.close(callback());
    
  }, function (ok, load) {

    ok(1, "created");
    load(__dirname + "/fixtures/create.after.json", callback("expected"));

  }, function (objectify) {

    objectify(tmp, callback("actual"));

  }, function (actual, expected) {

    say(actual);
    say(expected);

    deepEqual(actual, expected, "written");

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(callback());

  }, function (callback) {

    strata.iterator("a", callback("cursor"));

  }, function (callback, cursor, equal) {

    equal(cursor.length - cursor.offset, 0, "empty");
    //cursor.unlock()
    //strata.close(callback());

  });
});
