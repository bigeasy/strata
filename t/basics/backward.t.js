#!/usr/bin/env node

require('./proof')(5, function (Strata, tmp, serialize, equal, ok, step) {
    var fs = require ('fs'), strata, right
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator(strata.leftOf('c'), step())
    }, function (cursor) {
        ok(cursor.exclusive, 'exclusive')
        right = cursor.right
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            equal(got, 'a', 'go left')
            cursor.unlock()
        })
    }, function () {
        strata.mutator(strata.leftOf('d'), step())
    }, function (cursor) {
        equal(cursor.address, right, 'address and right')
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
