#!/usr/bin/env node

require("./proof")(2, function (step, ok, equal, Strata, tmp, deepEqual,
  say, die, serialize, gather, load, objectify) {
  var strata, cursor, purge, count = 0;

  step(function () {

    serialize(__dirname + '/fixtures/tree.before.json', tmp, step());

  }, function () {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());

  }, function () {

    strata.iterator('h', step());
    strata.iterator('h', step());

  }, function (first, second) {

    step(function () {
      first.get(first.offset, step());
    }, function (value) {
      ok(value, 'h', 'first');
      first.unlock();
    });

    step(function () {
      second.get(second.offset, step());
    }, function (value) {
      ok(value, 'h', 'second');
      second.unlock();
    });

  }, function() {

    strata.close(step());

  });
});

