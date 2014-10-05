#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
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
            cursor.unlock(step())
        })
    }, [function (records) {
        strata.balance(step())
    }, function (_, error) {
        assert(error.message, 'bogus', 'caught')
    }], function(actual, expected) {
        strata.close(step())
    })
})
