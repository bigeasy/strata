#!/usr/bin/env node

require('./proof')(4, function (async, Strata, tmp) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs'), records = [];
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/one.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function () {
    strata.iterator('a', async());
  }, function (cursor, equal) {
    equal(cursor.index, 0, 'found');
    equal(cursor.offset, 0, 'found');
    equal(cursor.length, 1, 'length');
    cursor.get(cursor.index, async());
  }, function (record, equal, cursor) {
    equal(record, 'a', 'records');
    cursor.unlock()
    strata.close(async());
  });
});
