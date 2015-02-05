#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('b', 'b', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.after.json', async())
    }, function(actual, expected) {
        assert.say(actual)
        assert.say(expected)

        assert(actual, expected, 'split')

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        assert(!strata.balanced, 'not balanced')

        strata.close(async())
    })
}
