#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/../basics/fixtures/split.before.json', tmp, async())
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
        })
    }, function () {
        strata.balance(async())
        async([function () {
            strata.balance(async())
        }, function (_, error) {
            assert(error.message, 'already balancing', 'error')
        }])
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function() {
        strata.close(async())
    })
})
