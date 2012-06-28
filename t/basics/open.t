#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(2, function (Strata, tmp, callback) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.create(callback());
}, function (callback) {
  strata.close(callback());
}, function (Strata, callback, tmp) {
  strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  strata.open(callback())
}, function (equal) {
  equal(strata.stats().size, 0, "json size");
  equal(strata.stats().nextAddress, 2, "next address");
});
