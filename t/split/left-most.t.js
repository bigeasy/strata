#!/usr/bin/env node

require('./proof')(3, function (async, assert) {
    var ok = require('assert').ok, strata = new Strata({
        directory: tmp,
        leafSize: 3,
        branchSize: 3,
        comparator: function (a, b) {
            ok(a != null && b != null, 'keys are null')
            return a < b ? - 1 : a > b ? 1 : 0
        }
    })
    async(function () {
        serialize(__dirname + '/fixtures/left-most.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('d', 'd', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'
        ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'
        ], 'records after balance')
        stringify(tmp, async())
    }, function (json) {
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-most.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
    }, function() {
        strata.close(async())
    })
})
