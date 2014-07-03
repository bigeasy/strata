#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, vivify, gather, say, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('a', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.indexOf('b', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'c', 'd' ], 'records')
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/left-empty.after.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)

        assert(actual, expected, 'merge')
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'c', 'd' ], 'merged')
    }, function() {
        strata.close(step())
    })
})
