#!/usr/bin/env node

require('./proof')(4, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/root-drain.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('b', 'b', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/root-drain.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
        strata.purge(0)
        assert(strata.size, 0, 'purged completely')
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ], 'records')
        strata.close(async())
    })
})
