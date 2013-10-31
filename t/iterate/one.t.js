#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, serialize, equal) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs'), records = []
    step(function () {
        serialize(__dirname + '/fixtures/one.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            equal(cursor.index, 0, 'found')
            equal(cursor.offset, 0, 'found')
            equal(cursor.length, 1, 'length')
            cursor.get(cursor.index, step())
        }, function (record) {
            equal(record, 'a', 'records')
            cursor.unlock()
            strata.close(step())
        })
    })
})
