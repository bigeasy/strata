#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/reinsert.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.insert('d', 'd', ~cursor.indexOf('d', cursor.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/reinsert.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after tree')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'balanced records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/reinsert.balanced.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'balanced tree')
        strata.close(async())
    })
}
