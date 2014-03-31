#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, objectify, serialize, deepEqual, gather, say) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
            gather(step, strata)
        })
    }, function (records) {
        step(function () {
            deepEqual(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(step())
        }, function () {
            objectify(tmp, step())
            load(__dirname + '/fixtures/merge.after.json', step())
        }, function (actual, expected) {
            say(expected)
            say(actual)

            deepEqual(actual, expected, 'merge')
        }, function () {
            gather(step, strata)
        }, function (records) {
            deepEqual(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(step())
        })
    })
})
