#!/usr/bin/env node

require('./proof')(2, function (step, assert) {
    var strata
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        assert(strata.size, 0, 'json size')
        assert(strata.nextAddress, 2, 'next address')
    })
})
