#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, objectify, gather, say, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.next(step())
        }, function () {
            cursor.indexOf('d', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c' ], 'records')
    }, function () {
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/leaf-less-than-max.after.json', step())
    }, function (expected, actual) {
        say(expected)
        say(actual)

        deepEqual(actual, expected, 'merge')
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c' ], 'merged')
        strata.close(step())
    })
})
