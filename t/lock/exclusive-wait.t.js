#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.iterator('h', step())
    }, function (cursor) {
        strata.mutator('h', step())
        cursor.unlock(step())
    }, function (cursor) {
        step(function () {
            cursor.get(cursor.offset, step())
        }, function (value) {
            assert(value, 'h', 'got')
            cursor.unlock(step())
        })
    }, function() {
        strata.close(step())
    })
})

