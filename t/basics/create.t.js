#!/usr/bin/env node

require('./proof')(5, function (async, assert) {
    var fs = require('fs'), strata
    async(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        assert(strata.size, 3, 'json size')
        strata.close(async())
    }, function () {
        assert(1, 'created')
        vivify(tmp, async())
        load(__dirname + '/fixtures/create.after.json', async())
    }, function (actual, expected) {
        assert.say(actual)
        assert.say(expected)

        assert(actual, expected, 'written')

        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        assert(cursor.length - cursor.offset, 0, 'empty')

        cursor.unlock(async())
    }, function () {
        strata.purge(0)
        assert(strata.size, 0, 'purged')

        strata.close(async())
    })
})
