#!/usr/bin/env node

require('./proof')(9, prove)

function prove (async, assert) {
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
            var item = cursor.get(cursor.offset)
            assert(item.record, 'a', 'get record')
            assert(item.key, 'a', 'get key')
            assert(strata.size, 54, 'json size after read')
            assert(item.heft, 54, 'record size')

            cursor.unlock(async())
        }, function () {
            strata.purge(0)
            assert(strata.size, 0, 'page')

            strata.close(async())
        })
    })
}
