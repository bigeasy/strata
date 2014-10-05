#!/usr/bin/env node

require('./proof')(3, function (step, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/leaf-remainder.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records')
        strata.balance(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h' ], 'records after balance')

        vivify(tmp, step())
        load(__dirname + '/fixtures/leaf-remainder.before.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
    }, function() {
        strata.close(step())
    })
})
