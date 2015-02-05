#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        strata.iterator('h', async())
        cursor.unlock(async())
    }, function (cursor) {
        assert(cursor.get(cursor.offset).record, 'h', 'got')
        cursor.unlock(async())
    }, function() {
        strata.close(async())
    })
}
