#!/usr/bin/env node

require('./proof')(2, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('h', async())
        strata.iterator('h', async())
    }, function (first, second) {
        async(function () {
            first.get(first.offset, async())
        }, function (value) {
            assert(value, 'h', 'first')
            first.unlock(async())
        })
        async(function () {
            second.get(second.offset, async())
        }, function (value) {
            assert(value, 'h', 'second')
            second.unlock(async())
        })
    }, function() {
        strata.close(async())
    })
})

