#!/usr/bin/env node

// Asserts that log replay will skip over the positions array.

require('./proof')(1, function (async, assert) {
    var strata
    async(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('a', 'a', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a' ], 'written')
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, replay: true })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        cursor.unlock(async())
    })
})
