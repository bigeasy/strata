#!/usr/bin/env node

require('./proof')(1, function (step, Strata, deepEqual, tmp, gather, serialize) {
    var strata = new Strata({ directory: tmp, branchSize: 3, leafSize: 3, readRecordStartLength: 2 })

    step(function () {
        serialize(__dirname + '/fixtures/read-record.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'c', 'd' ], 'records')
    }, function () {
        strata.close(step())
    })
})
