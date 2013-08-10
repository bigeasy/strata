#!/usr/bin/env node

require("./proof")(4, function (step, tmp, Strata, ok, equal, load, objectify, say, deepEqual) {
  var fs = require("fs"), strata;

  step(function () {

    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.create(step());

  }, function () {

    strata.mutator("a", step());

  }, function (cursor) {

    step(function () {

      cursor.insert("a", "a", ~ cursor.index, step());

    }, function (inserted) {

      equal(inserted, 0, "inserted");

      cursor.unlock()

      equal(strata.size, 32, "json size");

      strata.close(step());

    });

  }, function () {

    objectify(tmp, step());
    load(__dirname + "/fixtures/insert.json", step());

  }, function (actual, expected) {

    say(expected);
    say(actual);

    deepEqual(actual, expected, "insert");

    say(expected.segment00000001);

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(step());
  });
});
