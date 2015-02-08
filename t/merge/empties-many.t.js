#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/empties-many.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('eu', async())
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
        load(__dirname + '/fixtures/empties-many.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(async())
    })
}
