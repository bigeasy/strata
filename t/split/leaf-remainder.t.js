#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, vivify, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/leaf-remainder.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('b', 'b', ~ cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records')
        strata.balance(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records after balance')
        vivify(tmp, step())
        load(__dirname + '/fixtures/leaf-remainder.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
    }, function() {
        strata.close(step())
    })
})
