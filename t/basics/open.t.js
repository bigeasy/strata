#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        assert(strata.size, 0, 'json size')
        assert(strata.nextAddress, 2, 'next address')
    })
})
