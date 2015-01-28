#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/../basics/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            writeStage: 'tree'
        })
        strata.open(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.next(async())
        }, function () {
            cursor.indexOf('e', async())
        }, function (index) {
            cursor.insert('e', 'e', ~ index, async())
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
        strata = new Strata({
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
})
