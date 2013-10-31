#!/usr/bin/env node

require('./proof')(1, function (step, tmp, serialize, equal, load, Strata) {
    var fs = require('fs'), strata, records = []

    step(function () {
        serialize(__dirname + '/fixtures/resize.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.iterator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.get(cursor.index, step())
        }, function (got) {
            equal(got, 'b', 'loaded')
        })
    }, function (records) {
        strata.close(step())
    })
})
