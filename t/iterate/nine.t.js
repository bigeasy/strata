#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, deepEqual, serialize, equal, gather) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs')
    step(function () {
        serialize(__dirname + '/fixtures/nine.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        equal(cursor.index, 0, 'index')
        equal(cursor.offset, 0, 'offset')
        equal(cursor.length, 3, 'length')
        cursor.unlock()
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records')
    }, function() {
        strata.close(step())
    })
})
