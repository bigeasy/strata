#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator(strata.key('a'), async())
    }, function (cursor) {
        async(function () {
            cursor.get(cursor.offset, async())
        }, function (got) {
            cursor.unlock(async())
            assert(got, 'a', 'got')
        })
    }, function () {
        strata.close(async())
    })
})
