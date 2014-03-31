#!/usr/bin/env node

require('./proof')(9, function (step, Strata, tmp, serialize, assert) {
    var strata
    step(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        assert(strata.size, 0, 'json size before read')
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            assert(! cursor.exclusive, 'shared')
            assert(cursor.index, 0, 'index')
            assert(cursor.offset, 0, 'offset')
            cursor.get(cursor.offset, step())
        }, function (record, key, size) {
            assert(record, 'a', 'get record')
            assert(key, 'a', 'get key')
            assert(strata.size, 14, 'json size after read')
            assert(size, 54, 'record size')

            cursor.unlock()

            strata.purge(0)
            assert(strata.size, 0, 'page')

            strata.close(step())
        })
    })
})
