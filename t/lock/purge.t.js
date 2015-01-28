#!/usr/bin/env node

require('./proof')(4, function (async, assert) {
    var strata, count = 0

    function tracer (type, object, callback) {
        switch (type) {
        case 'reference':
            if (++count == 2) {
                assert(strata.size > 2, 'unpurged')
                strata.purge(0)
                assert(strata.size, 0, 'purged')
            }
            callback()
            break
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        async(function () {
            cursor.indexOf('h', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.indexOf('i', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        strata.mutator('e', async())
    }, function (cursor) {
        async(function () {
            cursor.indexOf('e', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.indexOf('g', async())
        }, function (index) {
            cursor.remove(index, async())
        }, function () {
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/tree.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'merge')
        strata.close(async())
    })
})
