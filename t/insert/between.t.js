#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/between.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function() {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('b', 'b', ~ cursor.index,  async())
        }, function () {
            cursor.unlock(async())
            vivify(tmp, async())
            load(__dirname + '/fixtures/between.after.json', async())
        })
    }, function (actual, expected) {
        assert(actual, expected, 'insert')
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c' ], 'records')
    }, function() {
        strata.close(async())
    })
}
