#!/usr/bin/env node

require('./proof')(4, function (async, assert) {
    var strata
    async(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        async(function () {
            cursor.insert('a', 'a', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            assert(strata.size, 14, 'json size')
        })
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/insert.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'insert')

        assert.say(expected.segment00000001)

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(async())
    })
})
