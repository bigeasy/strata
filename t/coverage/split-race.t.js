#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var cadence = require('../../cadence'),
        strata, count = 0

    function tracer (type, object, callback) {
        switch (type) {
        case 'plan':
            if (!count++) {
                cadence(function (async) {
                    async(function () {
                        strata.mutator('b', async())
                    }, function (cursor) {
                        async(function () {
                            cursor.remove(cursor.index)
                            cursor.remove(cursor.indexOf('c', cursor.ghosts))
                            cursor.remove(cursor.indexOf('d', cursor.ghosts))
                            cursor.unlock(async())
                        })
                    })
                })(callback)
                break
            }
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/split-race.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('d', 'd', ~ cursor.index)
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'records')
    }, function () {
        strata.balance(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/split-race.after.json', async())
    }, function(actual, expected) {
        assert(actual, expected, 'split')
        strata.close(async())
    })
}
