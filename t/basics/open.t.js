#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(2, function (Strata, tmp, step) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.create(step());
}, function (step) {
  strata.close(step());
}, function (Strata, step, tmp) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.open(step())
}, function (equal) {
  equal(strata.size, 0, "json size");
  equal(strata.nextAddress, 2, "next address");
});
