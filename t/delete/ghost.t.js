#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, load, serialize, vivify, gather, assert, say) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/delete.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('c', step())
    }, function (cursor) {
        step(function() {
            cursor.indexOf('c', step())
        }, function (i) {
            cursor.remove(i, step())
        }, function () {
            cursor.unlock(step())
        }, function () {
            gather(strata, step())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'deleted')
        vivify(tmp, step())
        load(__dirname + '/fixtures/ghost.after.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)
        assert(actual, expected, 'directory')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.next(step())
        }, function (next) {
            assert(cursor.offset, 1, 'ghosted')
            cursor.unlock(step())
        })
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'reopened')
        strata.close(step())
    })
})
