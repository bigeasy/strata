#!/usr/bin/env node

require('./proof')(3, function (async, Strata, tmp, deepEqual) {
  var strata = new Strata(tmp, { leafSize: 3, branchSize: 3 }), fs = require('fs'), ambiguity = [];
  async(function (serialize) { 
    serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async());
  }, function () {
    strata.open(async());
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records');
  }, function () {
    strata.mutator('a', async());
  }, function (cursor) {
    async(function page () {
      cursor.indexOf('z', async());
    }, function (index, cursor) {
      cursor.insert('z', 'z', ~index, async());
    }, function (unambiguous, cursor, page) {
      ambiguity.unshift(unambiguous);
      if (!ambiguity[0]) {
        cursor.next(async(page));
      } else {
        deepEqual(ambiguity, [ true, false, false, false ], 'unambiguous');
        cursor.unlock();
      }
    });
  }, function (gather) {
    gather(async, strata);
  }, function (records) {
    deepEqual(records, [ 'a', 'b', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert');
  }, function() {
    strata.close(async());
  });
});
