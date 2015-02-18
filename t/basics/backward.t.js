#!/usr/bin/env node

require('./proof')(5, prove)

function prove (async, assert) {
    var strata, right
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator(strata.leftOf('c'), async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        right = cursor.right.address
        assert(cursor.get(0).record, 'a', 'go left')
        cursor.unlock(async())
    }, function () {
        strata.mutator(strata.leftOf('d'), async())
    }, function (cursor) {
        assert(cursor.address, right, 'address and right')
        assert(!cursor.exclusive, 'shared')
        assert(cursor.get(0).record, 'c', 'go left missing')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
