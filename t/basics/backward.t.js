#!/usr/bin/env node

require('./proof')(5, function (async, assert) {
    var strata, right
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator(strata.leftOf('c'), async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        right = cursor.right
        async(function () {
            cursor.get(0, async())
        }, function (got) {
            assert(got, 'a', 'go left')
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator(strata.leftOf('d'), async())
    }, function (cursor) {
        assert(cursor.address, right, 'address and right')
        assert(!cursor.exclusive, 'shared')
        async(function () {
            cursor.get(0, async())
        }, function (got) {
            assert(got, 'c', 'go left missing')
            cursor.unlock(async())
        })
    }, function () {
        strata.close(async())
    })
})
