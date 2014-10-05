#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('f', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        strata.mutator('m', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/merge.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'after balance')
        console.log(require('util').inspect(actual, false, null))
        console.log(require('util').inspect(expected, false, null))
        strata.close(step())
    })
})
