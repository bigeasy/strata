#!/usr/bin/env node

require('./proof')(4, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.indexOf('b', async())
        }, function (index) {
            assert(~index <= cursor.length, 'unambiguous')
            cursor.insert('b', 'b', ~index, async())
        }, function () {
            cursor.indexOf('c', async())
        }, function (index) {
            assert(~index <= cursor.length, 'unambiguous cached')
            cursor.insert('c', 'c', ~index, async())
        }, function (unambiguous) {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function() {
        strata.close(async())
    })
})
