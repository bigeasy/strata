#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        assert(strata.sheaf.magazine.heft, 0, 'json size')
        assert(strata.sheaf.nextAddress, 2, 'next address')
    })
}
