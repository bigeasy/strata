#!/usr/bin/env node

require("./proof")(1, function (step, Strata, equal, ok, tmp) {
  
  var strata = new Strata(__dirname, {});

  step(function () {
    strata.create(step(Error));
  }, function (error) {
    equal(error.message, 'database /home/alan/git/ecma/strata/t/coverage is not empty.', 'directory not empty');
  });
});
