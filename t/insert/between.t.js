#!/usr/bin/env node

require('./proof')(2, function (step, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
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
            cursor.unlock(step())
            vivify(tmp, step())
            load(__dirname + '/fixtures/between.after.json', step())
        })
    }, function (actual, expected) {
        assert(actual, expected, 'insert')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c' ], 'records')
    }, function() {
        strata.close(step())
    })
})
