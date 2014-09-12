#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, load, vivify, assert, say) {
    var strata
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        step(function () {
            cursor.insert('a', 'a', ~ cursor.index, step())
        }, function () {
            cursor.unlock(step())
        }, function () {
            assert(strata.size, 14, 'json size')
        })
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/insert.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)

        assert(actual, expected, 'insert')

        say(expected.segment00000001)

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(step())
    })
})
