#!/usr/bin/env node

require("./proof")(1, function (serialize, tmp, equal, step, Strata, ok) {
  var fs = require("fs"), strata;

  step(function () {

    serialize(__dirname + "/../basics/fixtures/get.json", tmp, step());

  }, function () {

    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, nextTick: true });
    strata.open(step());

  }, function () {

    strata.iterator("a", step());

  }, function (cursor) {

    step(function () {

      cursor.get(cursor.offset, step());

    }, function (got) {

      equal(got, "a", "get");

      cursor.unlock();

      strata.close(step());
    });
  });
});
