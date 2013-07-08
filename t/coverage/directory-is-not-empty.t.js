#!/usr/bin/env node

require("./proof")(1, function (step, Strata, ok) {
  var strata = new Strata({ directory: __dirname });

  step(function () {
    strata.create(step(Error));
  }, function (error) {
    ok(/database .* is not empty\./.test(error.message), 'directory not empty');
  });
});
