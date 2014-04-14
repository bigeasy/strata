#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, serialize, gather, deepEqual, equal) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('l', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('z', step())
        }, function (index) {
            cursor.insert('z', 'z', ~index, step())
        }, function (unambiguous) {
            cursor.unlock()
            equal(unambiguous, 0, 'unambiguous')
        }, function () {
            gather(step, strata)
        })
    }, function (records) {
        deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert')
    }, function() {
        strata.close(step())
    })
})
