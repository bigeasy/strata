#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.iterator(strata.key('a'), step())
    }, function (cursor) {
        step(function () {
            cursor.get(cursor.offset, step())
        }, function (got) {
            cursor.unlock(step())
            assert(got, 'a', 'got')
        })
    }, function () {
        strata.close(step())
    })
})
