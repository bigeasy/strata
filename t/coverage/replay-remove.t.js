#!/usr/bin/env node

// Asserts that log replay will add and remove a record.

require('./proof')(1, prove)

function prove (async, assert) {
    var strata
    async(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('a', 'a', ~ cursor.index, async())
        }, function (inserted) {
            cursor.remove(0, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [], 'empty')
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        cursor.unlock(async())
    })
}
