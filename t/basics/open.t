#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(2, function (Strata, tmp, async) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.create(async());
}, function (async) {
  strata.close(async());
}, function (Strata, async, tmp) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.open(async())
}, function (equal) {
  equal(strata.stats.size, 0, "json size");
  equal(strata.stats.nextAddress, 2, "next address");
});
