#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, serialize, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('l', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('z', step())
        }, function (index) {
            assert(~index <= cursor.length, 'unambiguous')
            cursor.insert('z', 'z', ~index, step())
        }, function () {
            cursor.unlock(step())
        }, function () {
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert')
    }, function() {
        strata.close(step())
    })
})
