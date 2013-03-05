#!/usr/bin/env node

require("./proof")(4, function (step, tmp) {
  var fs = require("fs"), strata;

  step(function (Strata) {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(step());

  }, function () {

    strata.mutator("a", step());

  }, function (cursor) {

    var cassette = strata.cassette("a");
    cursor.insert(cassette.record, cassette.key, ~ cursor.index, step());

  }, function (inserted, cursor, ok, equal) {

    ok(inserted, "inserted");

    cursor.unlock()

    equal(strata.size, 32, "json size");

    strata.close(step());

  }, function (load) {

    load(__dirname + "/fixtures/insert.json", step());

  }, function (expected, objectify) {

    objectify(tmp, step());

  }, function (actual, expected, say, deepEqual) {

    say(expected);
    say(actual);

    deepEqual(actual, expected, "insert");

    say(expected.segment00000001);

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(step());
  });
});
