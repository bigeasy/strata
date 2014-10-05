#!/usr/bin/env node

require('./proof')(2, function (step, assert) {
    var strata
    step(function () {
        serialize(__dirname + '/../basics/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            writeStage: 'tree'
        })
        strata.open(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.next(step())
        }, function () {
            cursor.indexOf('e', step())
        }, function (index) {
            cursor.insert('e', 'e', ~ index, step())
        }, function () {
            cursor.unlock(step())
        }, function () {
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e' ], 'cached')
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            writeStage: 'leaf'
        })
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e' ], 'flushed')
    })
})
