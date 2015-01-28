#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/propagate.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('zz', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('zz', 'zz', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/propagate.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
        strata.close(async())
    })
})
