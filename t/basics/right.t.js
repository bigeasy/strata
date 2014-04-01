#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, serialize, equal, ok) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator(strata.right, step())
    }, function (cursor) {
        ok(cursor.exclusive, 'exclusive')
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            equal(got, 'c', 'right')
            cursor.unlock()
        })
    }, function () {
        strata.close(step())
    })
})
