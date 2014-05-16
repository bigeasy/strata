#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, serialize, load, objectify, gather, ok, equal, deepEqual) {
    var strata, count = 0

    function tracer (type, object, callback) {
        switch (type) {
        case 'reference':
            if (++count == 2) {
                ok(strata.size > 2, 'unpurged')
                strata.purge(0)
                equal(strata.size, 0, 'purged')
            }
            callback()
            break
        default:
            callback()
        }
    }

    step(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(step())
    }, function () {
        strata.mutator('h', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('h', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.indexOf('i', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        strata.mutator('e', step())
    }, function (cursor) {
        step(function () {
            cursor.indexOf('e', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.indexOf('g', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/tree.after.json', step())
    }, function (actual, expected) {
        deepEqual(actual, expected, 'merge')
        strata.close(step())
    })
})
