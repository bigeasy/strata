#!/usr/bin/env node

require('./proof')(3, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
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
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/right-ghost.after.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'after')
        strata.close(async())
    })
})
