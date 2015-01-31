#!/usr/bin/env node

require('./proof')(9, function (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        assert(strata.size, 0, 'json size before read')
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            assert(! cursor.exclusive, 'shared')
            assert(cursor.index, 0, 'index')
            assert(cursor.offset, 0, 'offset')
            cursor.get(cursor.offset, async())
        }, function (record, key, size) {
            assert(record, 'a', 'get record')
            assert(key, 'a', 'get key')
            assert(strata.size, 54, 'json size after read')
            assert(size, 54, 'record size')

            cursor.unlock(async())
        }, function () {
            strata.purge(0)
            assert(strata.size, 0, 'page')

            strata.close(async())
        })
    })
})
