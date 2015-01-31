#!/usr/bin/env node

require('./proof')(5, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/one.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            assert(cursor.index, 0, 'found')
            assert(cursor.offset, 0, 'offset')
            assert(cursor.ghosts, 0, 'ghosts')
            assert(cursor.length, 1, 'length')
            assert(cursor.get(cursor.index).record, 'a', 'records')
            cursor.unlock(async())
        }, function () {
            strata.close(async())
        })
    })
})
