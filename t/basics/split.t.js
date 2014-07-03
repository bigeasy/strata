#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, vivify, gather, assert, say) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, step())
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
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/split.after.json', step())
    }, function(actual, expected) {
        say(actual)
        say(expected)

        assert(actual, expected, 'split')

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(step())
    })
})
