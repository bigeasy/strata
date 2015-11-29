#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator(strata.right, async())
    }, function (cursor) {
        assert(cursor.exclusive, 'exclusive')
        assert(cursor.page.items[0].record, 'c', 'right')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
