#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/balancer-cached-right.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('e', async())
    }, function (cursor) {
        cursor.insert('e', 'e', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [  'b', 'c', 'd',  'e' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [  'b', 'c', 'd',  'e' ], 'merged')
    }, function() {
        strata.close(async())
    })
}
