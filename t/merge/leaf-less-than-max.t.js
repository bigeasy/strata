#!/usr/bin/env node

require('./proof')(3, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.next(async())
        }, function () {
            cursor.indexOf('d', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'c' ], 'records')
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/leaf-less-than-max.after.json', async())
    }, function (expected, actual) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'merge')
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'c' ], 'merged')
        strata.close(async())
    })
})
