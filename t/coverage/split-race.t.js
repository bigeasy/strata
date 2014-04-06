#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, serialize, load, objectify, gather, deepEqual) {
    var cadence = require('cadence'),
        strata, count = 0

    function tracer (type, object, callback) {
        switch (type) {
        case 'plan':
            if (!count++) {
                cadence(function (step) {
                    step(function () {
                        strata.mutator('b', step())
                    }, function (cursor) {
                        step(function () {
                            cursor.remove(cursor.index, step())
                        }, function () {
                            cursor.indexOf('c', step())
                        }, function (index) {
                            cursor.remove(index, step())
                        }, function () {
                            cursor.indexOf('d', step())
                        }, function (index) {
                            cursor.remove(index, step())
                        }, function () {
                            cursor.unlock()
                        })
                    })
                })(callback)
                break
            }
        default:
            callback()
        }
    }

    step(function () {
        serialize(__dirname + '/fixtures/split-race.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(step())
    }, function () {
        strata.mutator('d', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('d', 'd', ~ cursor.index, step())
        }, function () {
            cursor.unlock()
            gather(step, strata)
        })
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'records')
    }, function () {
        strata.balance(step())
    }, function () {
        strata.balance(step())
    }, function () {
        objectify(tmp, step())
        load(__dirname + '/fixtures/split-race.after.json', step())
    }, function(actual, expected) {
        deepEqual(actual, expected, 'split')
        strata.close(step())
    })
})
