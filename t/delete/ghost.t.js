#!/usr/bin/env node

require('./proof')(5, function (step, Strata, tmp, load, serialize, objectify, gather, equal, deepEqual, say) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/delete.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('c', step())
    }, function (cursor) {
        step(function() {
            cursor.indexOf('c', step())
        }, function (i) {
            cursor.remove(i, step())
        }, function () {
            cursor.unlock()
            gather(step, strata)
        })
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'd' ], 'deleted')
        objectify(tmp, step())
        load(__dirname + '/fixtures/ghost.after.json', step())
    }, function (actual, expected) {
        say(expected)
        say(actual)
        deepEqual(actual, expected, 'directory')
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
            equal(cursor.offset, 1, 'ghosted')
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'd' ], 'reopened')
        strata.close(step())
    })
})
