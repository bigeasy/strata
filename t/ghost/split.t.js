#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
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
        }, function (index) {
            cursor.insert('g', 'g', ~cursor._indexOf('g'), async())
        }, function () {
        }, function (index) {
            cursor.insert('h', 'h', ~cursor._indexOf('h'), async())
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
}
