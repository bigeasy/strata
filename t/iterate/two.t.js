#!/usr/bin/env node

require('./proof')(5, function (step, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), records = []
    step(function () {
        serialize(__dirname + '/fixtures/two.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            assert(cursor.index, 0, 'found')
            assert(cursor.offset, 0, 'found')
            assert(cursor.length, 2, 'length')
            cursor.get(cursor.index, step())
        }, function (record) {
            records.push(record)
            assert(cursor.index, 0, 'same index')
            cursor.get(cursor.index + 1, step())
        }, function (record) {
            records.push(record)
            assert(records, [ 'a', 'b' ], 'records')
            cursor.unlock(step())
        }, function () {
            strata.close(step())
        })
    })
})
