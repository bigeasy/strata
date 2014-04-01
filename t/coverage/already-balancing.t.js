#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, serialize, gather, equal, deepEqual) {
    var strata
    step(function () {
        serialize(__dirname + '/../basics/fixtures/split.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('b', 'b', ~ cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        strata.balance(step())
        step([function () {
            strata.balance(step())
        }, function (_, error) {
            equal(error.message, 'already balancing', 'error')
        }])
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function() {
        strata.close(step())
    })
})
