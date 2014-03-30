#!/usr/bin/env node

var fs = require('fs'), strata
require('./proof')(5, function (Strata, assert, say, tmp, step, load, objectify) {
    step(function () {

        fs.writeFile(tmp + '/.ignore', '', 'utf8', step())

    }, function () {

        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())

    }, function () {

        assert(strata.size, 3, 'json size')
        strata.close(step())

    }, function () {

        assert(1, 'created')
        objectify(tmp, step())
        load(__dirname + '/fixtures/create.after.json', step())

    }, function (actual, expected) {

        say(actual)
        say(expected)

        assert(actual, expected, 'written')

        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())

    }, function () {

        strata.iterator('a', step())

    }, function (cursor) {

        assert(cursor.length - cursor.offset, 0, 'empty')

        cursor.unlock()

        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(step())
    })
})
