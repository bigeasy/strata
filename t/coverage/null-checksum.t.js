#!/usr/bin/env node

require("./proof")(1, function (step, Strata, equal, ok, tmp) {
  
  var fs = require('fs'), path = require('path');
  var strata = new Strata(tmp, { checksum: "none" }) ;

  step(function () {
    strata.create(step());
  }, function () {
    strata.close(step());
  }, function () {
    fs.readFile(path.join(tmp, 'segment00000000'), 'utf8', step());
  }, function (body) {
    equal(+(body.split(/\n/)[0].split(/\s+/)[1]), 0, 'zero');
  });
});
