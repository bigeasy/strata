#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, gather, objectify, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/exorcise.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('c', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function() {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'records')
        strata.balance(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'merged')
        objectify(tmp, step())
        load(__dirname + '/fixtures/exorcise.after.json', step())
    }, function (actual, expected) {
        deepEqual(actual, expected, 'after')
        strata.close(step())
    })
})
