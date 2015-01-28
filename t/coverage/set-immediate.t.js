#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/../basics/fixtures/get.json', tmp, async())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            setImmediate: true
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.get(cursor.offset, async())
        }, function (got) {
            assert(got, 'a', 'get')
            cursor.unlock(async())
        }, function () {
            strata.close(async())
        })
    })
})
