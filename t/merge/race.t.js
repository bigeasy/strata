#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var cadence = require('cadence'), strata, insert

    function tracer (type, object, callback) {
        switch (type) {
        case 'plan':
            cadence(function (async) {
                async(function () {
                    strata.mutator(insert, async())
                }, function (cursor) {
                    async(function () {
                        cursor.insert(insert, insert, ~cursor.index, async())
                    }, function () {
                        cursor.unlock(async())
                    })
                })
            })(callback)
            break
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        insert = 'b'
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/race-left.after.json', async())
    }, function(actual, expected) {
        assert(actual, expected, 'race left')
        strata.close(async())
    }, function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        insert = 'd'
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/race-right.after.json', async())
    }, function(actual, expected) {
        assert(actual, expected, 'race right')
        strata.close(async())
    })
}
