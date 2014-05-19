#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, serialize, load, objectify, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/empties-non-pivot.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('cr', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/empties-non-pivot.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(step())
    })
})
