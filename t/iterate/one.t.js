#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, serialize, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/one.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            assert(cursor.index, 0, 'found')
            assert(cursor.offset, 0, 'offset')
            assert(cursor.ghosts, 0, 'ghosts')
            assert(cursor.length, 1, 'length')
            cursor.get(cursor.index, step())
        }, function (record) {
            assert(record, 'a', 'records')
            cursor.unlock(step())
        }, function () {
            strata.close(step())
        })
    })
})
