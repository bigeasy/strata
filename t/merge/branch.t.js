#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, vivify, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/branch.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('h', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('h', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.indexOf('i', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/branch.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'merge')
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'j', 'k', 'l', 'm', 'n' ], 'merged')
    }, function() {
        strata.close(step())
    })
})
