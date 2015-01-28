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
            cursor.indexOf('c', async())
        }, function (index) {
            cursor.remove(index, async())
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
        assert(records, [ 'a', 'b' ], 'records')

        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/right-empty.after.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)

        assert(actual, expected, 'merge')
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b' ], 'merged')
    }, function() {
        strata.close(async())
    })
})
