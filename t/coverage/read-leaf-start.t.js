#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata = createStrata({
        directory: tmp,
        branchSize: 3,
        leafSize: 3,
        readLeafStartLength: 128
    })
    async(function () {
        serialize(__dirname + '/fixtures/read-record.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'c', 'd' ], 'records')
    }, function () {
        strata.close(async())
    })
}
