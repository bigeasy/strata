#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/left-singles.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('bt', async())
    }, function (cursor) {
        async(function () {
            cursor.indexOf('bt', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.indexOf('bu', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('bw', async())
    }, function (cursor) {
        async(function () {
            cursor.indexOf('bw', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-singles.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'merged')
    }, function () {
        strata.close(async())
    })
})
