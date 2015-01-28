#!/usr/bin/env node

require('./proof')(4, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.indexOf('g', async())
        }, function (index) {
            cursor.insert('g', 'g', ~index, async())
        }, function () {
            cursor.indexOf('h', async())
        }, function (index) {
            cursor.insert('h', 'h', ~index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g', 'h' ], 'records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after tree')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g', 'h' ], 'balanced records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.balanced.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after balance')
        strata.close(async())
    })
})
