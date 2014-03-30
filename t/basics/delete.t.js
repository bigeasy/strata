#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp,  serialize, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'a', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('c', step())
        }, function (i) {
            cursor.remove(i, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'a', 'd' ], 'records')

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(step())
    })
})
