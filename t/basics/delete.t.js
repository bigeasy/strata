#!/usr/bin/env node

require('./proof')(3, function (Strata, step, tmp,  load, objectify, serialize, gather, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('c', step())
        }, function (i) {
            cursor.remove(i, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'd' ], 'records')

        strata.purge(0)
        deepEqual(strata.size, 0, 'purged')

        strata.close(step())
    })
})
