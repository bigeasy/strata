#!/usr/bin/env node

require('./proof')(4, function (Strata, tmp, serialize, equal, ok, step) {
    var fs = require ('fs'), strata
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator(strata.leftOf('c'), step())
    }, function (cursor) {
        ok(cursor.exclusive, 'exclusive')
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            equal(got, 'a', 'go left')
            cursor.unlock()
        })
    }, function () {
        strata.mutator(strata.leftOf('d'), step())
    }, function (cursor) {
        ok(!cursor.exclusive, 'shared')
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            equal(got, 'c', 'go left missing')
            cursor.unlock()
        })
    }, function () {
        strata.close(step())
    })
})
