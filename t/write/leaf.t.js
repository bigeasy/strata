#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/../basics/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            writeStage: 'leaf'
        })
        strata.open(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.next(async())
        }, function () {
            cursor.insert('e', 'e', ~cursor._indexOf('e'), async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e' ], 'cached')
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            writeStage: 'leaf'
        })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e' ], 'flushed')
    })
}
