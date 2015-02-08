#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/branch.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor._indexOf('h'), async())
        }, function () {
            cursor.remove(cursor._indexOf('i'), async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('e', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor._indexOf('e'), async())
        }, function () {
            cursor.remove(cursor._indexOf('g'), async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('m', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor._indexOf('m'), async())
        }, function () {
            cursor.remove(cursor._indexOf('n'), async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l' ], 'records')
        strata.balance(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/root-fill.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'merge')
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'f', 'j', 'k', 'l' ], 'merged')
    }, function() {
        strata.close(async())
    })
}
