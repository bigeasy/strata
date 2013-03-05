#!/usr/bin/env node
var fs = require('fs');
require('./proof')(3, function (Strata, step, tmp,  load, objectify, _) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
  step(function (serialize) {
    serialize(__dirname + '/fixtures/split.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function (step, gather) {
    gather(step, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "c", "d" ], "records");
  }, function () {
    strata.mutator("a", step());
  }, function (cursor) {
    cursor.indexOf("c", step())
  }, function (i, cursor) {
    cursor.remove(i, step());
  }, function (step, gather, cursor) {
    cursor.unlock()
    gather(step, strata);
  }, function (records, deepEqual) {
    deepEqual(records, [ "a", "d" ], "records");

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(step());
  });
});
