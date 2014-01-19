#!/usr/bin/env node

var fs = require('fs'), strata
require('./proof')(9, function (serialize, tmp, equal, step, Strata, ok) {
    step(function () {

        serialize(__dirname + '/fixtures/get.json', tmp, step())

    }, function () {

        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())

    }, function () {

        equal(strata.size, 0, 'json size before read')

        strata.iterator('a', step())

    }, function (cursor) {

        step(function () {

            ok(! cursor.exclusive, 'shared')
            equal(cursor.index, 0, 'index')
            equal(cursor.offset, 0, 'offset')

            cursor.get(cursor.offset, step())

        }, function (record, key, size) {

            equal(record, 'a', 'get record')
            equal(key, 'a', 'get key')
            equal(strata.size, 14, 'json size after read')
            equal(size, 54, 'record size')

            cursor.unlock()

            strata.purge(0)
            equal(strata.size, 0, 'page')

            strata.close(step())
        })
    })
})
