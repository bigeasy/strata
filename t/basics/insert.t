#!/usr/bin/env node

require("./proof")(3, function (callback, tmp) {
  var fs = require("fs"), strata;

  callback(function (Strata) {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(callback());

  }, function () {

    strata.mutator("a", callback("cursor"));

  }, function (cursor) {

    var cassette = strata.cassette("a");
    cursor.insert(cassette.record, cassette.key, ~ cursor.index, callback("inserted"));

  }, function (cursor, inserted, ok, equal) {

    ok(inserted, "inserted");

    cursor.unlock()

    equal(strata.stats.size, 32, "json size");

    strata.close(callback());

  }, function (load, objectify) {

    load(__dirname + "/fixtures/insert.json", callback("expected"));
    objectify(tmp, callback("actual"));

  }, function (actual, expected, say, deepEqual) {

    say(expected);
    say(actual);

    deepEqual(actual, expected, "insert");

    say(expected.segment00000001);

  });
});
