#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, deepEqual, serialize, gather, load, objectify) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), fs = require('fs')
    step(function () {
        serialize(__dirname + '/fixtures/between.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function() {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('b', 'b', ~ cursor.index,  step())
        }, function () {
            cursor.unlock()
            objectify(tmp, step())
            load(__dirname + '/fixtures/between.after.json', step())
        })
    }, function (actual, expected) {
        deepEqual(actual, expected, 'insert')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c' ], 'records')
    }, function() {
        strata.close(step())
    })
})
