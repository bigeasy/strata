#!/usr/bin/env node

require("./proof")(4, function (step, ok, equal, Strata, tmp, deepEqual, say, die) {
  var strata, purge, count = 0;

  function tracer (trace, callback) {
    switch (trace.type) {
    case "reference":
      if (++count == 2) {
        ok(trace.report().cache.length > 2, "unpurged");
        strata.purge(0);
        equal(0, trace.report().cache.length, "purged");
      }
      callback();
      break;
    default:
      say(trace.type);
      callback();
    }
  }

  step(function (serialize) {

    serialize(__dirname + '/fixtures/tree.before.json', tmp, step());

  }, function () {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3, tracer: tracer });
    strata.open(step());

  }, function () {

    strata.mutator('h', step());

  }, function (cursor) {

    step(function () {

      cursor.indexOf('h', step());

    }, function (index) {

      cursor.remove(index, step());

    }, function () {

      cursor.indexOf('i', step());

    }, function (index) {

      cursor.remove(index, step());
      cursor.unlock();

    });
  }, function () {

    strata.mutator('e', step());

  }, function (cursor) {

    step(function () {

      cursor.indexOf('e', step());

    }, function (index) {

      cursor.remove(index, step());

    }, function () {

      cursor.indexOf('g', step());

    }, function (index) {

      cursor.remove(index, step());
      cursor.unlock();

    });
  }, function (gather) {

    gather(step, strata);

  }, function (records) {

    deepEqual(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l', 'm', 'n' ], 'records');
    strata.balance(step());

  }, function (load) {

    load(__dirname + '/fixtures/tree.after.json', step());

  }, function (expected, objectify) {

    objectify(tmp, step());

  }, function (actual, expected, say) {

    deepEqual(actual, expected, 'merge');

    strata.close(step());

  });
});
