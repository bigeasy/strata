#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var path = require('path'), strata = new Strata({
        directory: tmp,
        leafSize: 3,
        branchSize: 3,
        tracer: tracer
    })
    function tracer (type, object, callback) {
        if (type == 'readRecord') {
            callback(new Error('bogus error'))
        } else {
            callback()
        }
    }
    step(function () {
        serialize(path.join(__dirname, '/fixtures/split-race.before.json'), tmp, step())
    }, function () {
        strata.open(step())
    },[function () {
        step(function () {
            strata.iterator('a', step())
        }, function (cursor) {
            cursor.unlock()
        })
    }, function (_, error) {
        assert(error.message, 'bogus error', 'error on read record')
    }], function () {
        strata.close(step())
    })
})
