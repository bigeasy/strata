#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator('h', step())
    }, function (cursor) {
        strata.iterator('h', step())
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

