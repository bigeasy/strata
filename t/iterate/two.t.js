#!/usr/bin/env node

require('./proof')(5, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), records = []
    async(function () {
        serialize(__dirname + '/fixtures/two.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            assert(cursor.index, 0, 'found')
            assert(cursor.offset, 0, 'found')
            assert(cursor.length, 2, 'length')
            var record = cursor.get(cursor.index).record
            records.push(record)
            assert(cursor.index, 0, 'same index')
            var record = cursor.get(cursor.index + 1).record
            records.push(record)
            assert(records, [ 'a', 'b' ], 'records')
            cursor.unlock(async())
        }, function () {
            strata.close(async())
        })
    })
})
