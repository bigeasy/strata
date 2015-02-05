#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/exorcise.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function() {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/exorcise.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(async())
    })
}
