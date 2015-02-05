#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (a) {
        async(function () {
            a.insert('a', 'a', ~ a.index, async())
        }, function () {
            strata.mutator('b', async())
            a.unlock(async())
        }, function (b) {
            async(function () {
                b.insert('b', 'b', ~ b.index, async())
            }, function () {
                b.unlock(async())
            }, function () {
                gather(strata, async())
            })
        })
    }, function (records) {
        assert(records, [ 'a', 'b' ], 'records')
        strata.close(async())
    })
}
