#!/usr/bin/env node

require("./proof")(1, function (step, Strata, deepEqual, tmp, insert, load, objectify, equal, serialize) {
  var strata = new Strata(tmp, { fs: proxy, leafSize: 3, readBufferStartSize: 2 });

  step(function () {
    serialize(__dirname + "/fixtures/read-record.before.json", tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ "a", "b", "c", "d" ], "records");
  }, function () {
    strata.close(step());
  });
});
