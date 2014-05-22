#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, load, serialize, objectify, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/empties-pivot.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('ay', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        console.log('before balance')
        strata.balance(step())
    }, function () {
        console.log('after balance')
        objectify(tmp, step())
        load(__dirname + '/fixtures/empties-pivot.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(step())
    })
})
