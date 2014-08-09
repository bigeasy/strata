#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, serialize, equal) {
    var strata
    step(function () {
        serialize(__dirname + '/../basics/fixtures/get.json', tmp, step())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            setImmediate: true
        })
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.get(cursor.offset, step())
        }, function (got) {
            equal(got, 'a', 'get')
            cursor.unlock(step())
        }, function () {
            strata.close(step())
        })
    })
})
