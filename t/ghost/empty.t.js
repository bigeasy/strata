#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/empty.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('f', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('i', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/empty.after.json', async())
    }, function (actual, expected) {
        console.log(require('util').inspect(actual, false, null))
        console.log(require('util').inspect(expected, false, null))
        assert(actual, expected, 'after balance')
        strata.close(async())
    })
})
