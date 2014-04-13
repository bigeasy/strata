#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, load, serialize, objectify, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/first.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/first.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.vivify(step())
    }, function (result) {
        assert(result, [ { address: 1, children: [ 'b', 'c' ], ghosts: 0 } ], 'ghostbusters')
    }, function () {
        strata.close(step())
    })
})
