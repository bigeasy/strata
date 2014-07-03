#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, serialize, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('g', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock()
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'h', 'i', 'l', 'm', 'n' ], 'records after delete')
        strata.mutator('j', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('j', 'j', ~cursor.index, step())
        }, function (unambiguous) {
            assert(unambiguous, 0, 'unambiguous')
            cursor.unlock()
        }, function () {
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'h', 'i', 'j', 'l', 'm', 'n' ], 'records after insert')
    }, function() {
        strata.close(step())
    })
})
