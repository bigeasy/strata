#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        assert(strata.size, 44, 'before purge')
        strata.purge(0)
        assert(strata.size, 21, 'after purge')
        cursor.unlock(async())
    }, function() {
        strata.close(async())
    })
})

