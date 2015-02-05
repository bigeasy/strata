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
        strata.mutator('g', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'h', 'i', 'l', 'm', 'n' ], 'records after delete')
        strata.mutator('j', async())
    }, function (cursor) {
        async(function () {
            var index = cursor._indexOf('j')
            assert(~index <= cursor.length, 'unambiguous')
            cursor.insert('j', 'j', ~cursor.index, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'h', 'i', 'j', 'l', 'm', 'n' ], 'records after insert')
    }, function() {
        strata.close(async())
    })
}
