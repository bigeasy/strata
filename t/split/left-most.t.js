#!/usr/bin/env node

require('./proof')(3, function (step, assert) {
    var ok = require('assert').ok, strata = new Strata({
        directory: tmp,
        leafSize: 3,
        branchSize: 3,
        comparator: function (a, b) {
            ok(a != null && b != null, 'keys are null')
            return a < b ? - 1 : a > b ? 1 : 0
        }
    })
    step(function () {
        serialize(__dirname + '/fixtures/left-most.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('d', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('d', 'd', ~ cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'
        ], 'records')
        strata.balance(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p'
        ], 'records after balance')
        stringify(tmp, step())
    }, function (json) {
        vivify(tmp, step())
        load(__dirname + '/fixtures/left-most.after.json', step())
    }, function (actual, expected) {
        assert(actual, expected, 'split')
    }, function() {
        strata.close(step())
    })
})
