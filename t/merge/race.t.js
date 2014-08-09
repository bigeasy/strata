#!/usr/bin/env node

require('./proof')(2, function (step, Strata, tmp, load, serialize, vivify, assert) {
    var cadence = require('cadence'), strata, insert

    function tracer (type, object, callback) {
        switch (type) {
        case 'plan':
            cadence(function (step) {
                step(function () {
                    strata.mutator(insert, step())
                }, function (cursor) {
                    step(function () {
                        cursor.insert(insert, insert, ~cursor.index, step())
                    }, function () {
                        cursor.unlock(step())
                    })
                })
            })(callback)
            break
        default:
            callback()
        }
    }

    step(function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(step())
    }, function () {
        strata.mutator('b', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        insert = 'b'
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/race-left.after.json', step())
    }, function(actual, expected) {
        assert(actual, expected, 'race left')
        strata.close(step())
    }, function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(step())
    }, function () {
        strata.mutator('d', step())
    }, function (cursor) {
        step(function () {
            cursor.remove(cursor.index, step())
        }, function () {
            cursor.unlock(step())
        })
    }, function () {
        insert = 'd'
        strata.balance(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/race-right.after.json', step())
    }, function(actual, expected) {
        assert(actual, expected, 'race right')
        strata.close(step())
    })
})
