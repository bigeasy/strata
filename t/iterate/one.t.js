#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, serialize, equal) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/one.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            equal(cursor.index, 0, 'found')
            equal(cursor.offset, 0, 'offset')
            equal(cursor.ghosts, 0, 'ghosts')
            equal(cursor.length, 1, 'length')
            cursor.get(cursor.index, step())
        }, function (record) {
            equal(record, 'a', 'records')
            cursor.unlock()
            strata.close(step())
        })
    })
})
