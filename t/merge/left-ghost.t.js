#!/usr/bin/env node

require('./proof')(3, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/left-ghost.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function() {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-ghost.after.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'after')
        strata.close(async())
    })
}
