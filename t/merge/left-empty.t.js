#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
        }, function (index) {
            cursor.remove(cursor._indexOf('a'), async())
        }, function () {
            cursor.remove(cursor._indexOf('b'), async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'c', 'd' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-empty.after.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'merge')
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'c', 'd' ], 'merged')
    }, function() {
        strata.close(async())
    })
}
