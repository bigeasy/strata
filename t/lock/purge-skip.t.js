#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, serialize, equal) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator('h', step())
    }, function (cursor) {
        equal(strata.size, 41, 'before purge')
        strata.purge(0)
        equal(strata.size, 18, 'after purge')
        cursor.unlock(step())
    }, function() {
        strata.close(step())
    })
})

