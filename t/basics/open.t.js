#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, equal) {
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
        equal(strata.size, 0, 'json size')
        equal(strata.nextAddress, 2, 'next address')
    })
})
