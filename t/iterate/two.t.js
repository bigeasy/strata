#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, deepEqual, serialize, equal) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs'), records = [];
  step(function () {
    serialize(__dirname + '/fixtures/two.json', tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    strata.iterator('a', step());
  }, function (cursor) {
    step(function () {
      equal(cursor.index, 0, 'found');
      equal(cursor.offset, 0, 'found');
      equal(cursor.length, 2, 'length');
      cursor.get(cursor.index, step());
    }, function (record) {
      records.push(record);
      equal(cursor.index, 0, 'same index');
      cursor.get(cursor.index + 1, step());
    }, function (record) {
      records.push(record);
      deepEqual(records, [ 'a', 'b' ], 'records');
      cursor.unlock();
      strata.close(step());
    });
  });
});
