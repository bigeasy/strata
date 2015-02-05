#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/unlocker.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('ba', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/unlocker.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'balanced')
        strata.close(async())
    })
}
