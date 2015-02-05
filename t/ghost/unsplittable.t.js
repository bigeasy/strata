#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/unsplittable.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('g', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('g', 'g', ~cursor.index, async())
        }, function () {
            cursor.indexOf('d', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'balanced')
        vivify(tmp, async())
        load(__dirname + '/fixtures/unsplittable.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(async())
    })
}
