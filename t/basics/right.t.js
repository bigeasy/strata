#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator(strata.right, async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        async(function () {
            cursor.get(0, async())
        }, function (got) {
            assert(got, 'c', 'right')
            cursor.unlock(async())
        })
    }, function () {
        strata.close(async())
    })
})
