#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, serialize, assert) {
    var strata
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
            assert(got, 'b', 'loaded')
            cursor.unlock(step())
        })
    }, function () {
        strata.close(step())
    })
})
