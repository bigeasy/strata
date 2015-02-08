#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        cursor.insert('a', 'a', ~cursor.index)
        cursor.unlock(async())
    }, function () {
            assert(strata.size, 54, 'json size')
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
}
