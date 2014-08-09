#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, vivify, gather, assert) {
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
            cursor.unlock(step())
        })
    }, function() {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'records')
        strata.balance(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'merged')
        vivify(tmp, step())
        load(__dirname + '/fixtures/exorcise.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(step())
    })
})
