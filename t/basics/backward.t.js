#!/usr/bin/env node

require('./proof')(5, function (step, assert) {
    var strata, right
    step(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.mutator(strata.leftOf('c'), step())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        right = cursor.right
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            assert(got, 'a', 'go left')
            cursor.unlock(step())
        })
    }, function () {
        strata.mutator(strata.leftOf('d'), step())
    }, function (cursor) {
        assert(cursor.address, right, 'address and right')
        assert(!cursor.exclusive, 'shared')
        step(function () {
            cursor.get(0, step())
        }, function (got) {
            assert(got, 'c', 'go left missing')
            cursor.unlock(step())
        })
    }, function () {
        strata.close(step())
    })
})
