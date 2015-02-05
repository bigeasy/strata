#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/resize.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('b', async())
    }, function (cursor) {
        assert(cursor.get(cursor.index).record, 'b', 'loaded')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
