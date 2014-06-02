#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, load, serialize, objectify, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/branch.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('n', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('n', 'n', ~ cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/branch.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
        strata.purge(0)
        assert(strata.size, 0, 'purge completely')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.close(step())
    })
})
