#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, load, serialize, vivify, gather, say, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            assert(cursor.index, 0, 'unghostable')
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'b', 'c', 'd' ], 'records')
        strata.balance(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'b', 'c', 'd' ], 'merged')
        vivify(tmp, step())
        load(__dirname + '/fixtures/left-most-unghostable.after.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)

        assert(actual, expected, 'after')
        strata.close(step())
    })
})
