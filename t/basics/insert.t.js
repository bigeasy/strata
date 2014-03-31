#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, load, objectify, ok, equal, deepEqual, say) {
    var strata
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        ok(cursor.exclusive, 'exclusive')
        step(function () {
            cursor.insert('a', 'a', ~ cursor.index, step())
        }, function (inserted) {
            equal(inserted, 0, 'inserted')
            cursor.unlock()
            equal(strata.size, 14, 'json size')
        })
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/insert.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)

        deepEqual(actual, expected, 'insert')

        say(expected.segment00000001)

        strata.purge(0)
        deepEqual(strata.size, 0, 'purged')

        strata.close(step())
    })
})
