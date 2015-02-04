#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata

    function tracer (type, object, callback) {
        switch (type) {
        case 'lock':
            if (object.address == 0 && object.exclusive) {
                callback(new Error('bogus'))
                break
            }
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/split-race.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('d', 'd', ~ cursor.index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, [function (records) {
        strata.balance(async())
    }, function (error) {
        assert(error.message, 'bogus', 'caught')
    }], function(actual, expected) {
        strata.close(async())
    })
})
