#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs');
  step(function (serialize) { 
    serialize(__dirname + '/fixtures/nine.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.iterator('a', step());
  }, function (cursor, equal) {
    equal(cursor.index, 0, 'index');
    equal(cursor.offset, 0, 'offset');
    equal(cursor.length, 3, 'length');
  }, function (gather) {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records');
  }, function() {
    strata.close(step());
  });
});
