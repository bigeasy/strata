#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index)
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        async(function () {
            assert(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(async())
        }, function () {
            vivify(tmp, async())
            load(__dirname + '/fixtures/merge.after.json', async())
        }, function (actual, expected) {
            assert.say(expected)
            assert.say(actual)

            assert(actual, expected, 'merge')
        }, function () {
            gather(strata, async())
        }, function (records) {
            assert(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(async())
        })
    })
}
