#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/branch.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('n', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('n', 'n', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/branch.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
        strata.purge(0)
        assert(strata.size, 0, 'purge completely')
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.close(async())
    })
}
