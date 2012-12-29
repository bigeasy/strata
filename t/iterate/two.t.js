#!/usr/bin/env node

require('./proof')(5, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs'), records = [];
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/two.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.iterator('a', async());
  }, function (cursor, equal) {
    equal(cursor.index, 0, 'found');
    equal(cursor.offset, 0, 'found');
    equal(cursor.length, 2, 'length');
    cursor.get(cursor.index, async());
  }, function (record, cursor, equal) {
    records.push(record);
    equal(cursor.index, 0, 'same index');
    cursor.get(cursor.index + 1, async());
  }, function (record, cursor) {
    records.push(record);
    deepEqual(records, [ 'a', 'b' ], 'records');
    cursor.unlock();
    strata.close(async());
  });
});
