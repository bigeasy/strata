#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(2, function (Strata, tmp, step, equal) {
  step(function () {
    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.create(step());
  }, function () {
    strata.close(step());
  }, function () {
    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.open(step())
  }, function () {
    equal(strata.size, 0, "json size");
    equal(strata.nextAddress, 2, "next address");
  });
});
