#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, objectify, gather, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/leaf-remainder.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records')
        strata.balance(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records after balance')

        objectify(tmp, step())
        load(__dirname + '/fixtures/leaf-remainder.before.json', step())
    }, function (actual, expected) {
        deepEqual(actual, expected, 'split')
    }, function() {
        strata.close(step())
    })
})
