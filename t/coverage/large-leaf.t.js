#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/large-leaf.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('_', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('_', '_', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, '_abcdefghijklmnopqrstuvwxyz'.split(''), 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/large-leaf.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
        strata.close(async())
    })
}
