#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, load, serialize, objectify, gather, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs')
    step(function () {
        serialize(__dirname + '/fixtures/root-drain.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('b', 'b', ~ cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ], 'records')
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/root-drain.after.json', step())
    }, function (actual, expected) {
        deepEqual(actual, expected, 'split')
        strata.purge(0)
        deepEqual(strata.size, 0, 'purged completely')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ], 'records')
        strata.close(step())
    })
})
