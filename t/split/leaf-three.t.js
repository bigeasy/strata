#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/leaf-three.before.json', tmp, async())
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
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/leaf-three.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
    }, function() {
        strata.close(async())
    })
})
