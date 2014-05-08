#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        strata.create(step())
    }, function () {
        strata.mutator('a', step())
    }, function (a) {
        step(function () {
            a.insert('a', 'a', ~ a.index, step())
        }, function () {
            strata.mutator('b', step())
            a.unlock()
        }, function (b) {
            step(function () {
                b.insert('b', 'b', ~ b.index, step())
            }, function () {
                b.unlock()
                gather(step, strata)
            })
        })
    }, function (records) {
        assert(records, [ 'a', 'b' ], 'records')
        strata.close(step())
    })
})
