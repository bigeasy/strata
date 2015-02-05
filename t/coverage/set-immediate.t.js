#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
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
        assert(cursor.get(cursor.offset).record, 'a', 'get')
        cursor.unlock(async())
    })
}
