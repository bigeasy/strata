#!/usr/bin/env node

require('./proof')(4, prove)

function prove (async, assert) {
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
            var index = cursor._indexOf('b')
            assert(~index <= cursor.length, 'unambiguous')
            cursor.insert('b', 'b', ~index, async())
        }, function () {
            var index = cursor._indexOf('c')
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
}
