#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, deepEqual, serialize, gather, equal) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs')
    step(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('b', step())
        }, function (index) {
            cursor.insert('b', 'b', ~index, step())
        }, function (unambiguous) {
            cursor.unlock()
            equal(unambiguous, 0, 'unambiguous')
            cursor.indexOf('c', step())
        }, function (index) {
            cursor.insert('c', 'c', ~index, step())
        }, function (unambiguous) {
            cursor.unlock()
            equal(unambiguous, 0, 'unambiguous cached')
        }, function () {
            gather(step, strata)
        })
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function() {
        strata.close(step())
    })
})
