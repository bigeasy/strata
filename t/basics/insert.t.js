#!/usr/bin/env node

require("./proof")(4, function (async, tmp) {
  var fs = require("fs"), strata;

  async(function (Strata) {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(async());

  }, function () {

    strata.mutator("a", async());

  }, function (cursor) {

    var cassette = strata.cassette("a");
    cursor.insert(cassette.record, cassette.key, ~ cursor.index, async());

  }, function (inserted, cursor, ok, equal) {

    ok(inserted, "inserted");

    cursor.unlock()

    equal(strata.size, 32, "json size");

    strata.close(async());

  }, function (load) {

    load(__dirname + "/fixtures/insert.json", async());

  }, function (expected, objectify) {

    objectify(tmp, async());

  }, function (actual, expected, say, deepEqual) {

    say(expected);
    say(actual);

    deepEqual(actual, expected, "insert");

    say(expected.segment00000001);

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(async());
  });
});
