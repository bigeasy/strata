#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, deepEqual, serialize, gather, equal) {
  var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs'), ambiguity = [];
  step(function () {
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('a', step());
  }, function (cursor) {
    var page;
    var page = step(function () {
      cursor.indexOf('z', step());
    }, function (index) {
      cursor.insert('z', 'z', ~index, step());
    }, function (unambiguous) {
      ambiguity.unshift(unambiguous);
      if (ambiguity[0]) {
        cursor.next(step(page, 0));
      } else {
        deepEqual(ambiguity, [ 0, 1, 1, 1 ], 'unambiguous');
        cursor.unlock();
      }
    })(1);
  }, function () {
    gather(step, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert');
  }, function() {
    strata.close(step());
  });
});
