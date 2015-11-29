#!/usr/bin/env node

require('./proof')(5, prove)

function prove (async, assert) {
    var fs = require('fs'), strata
    async(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        assert(strata.sheaf.magazine.heft, 0, 'total heft')
        strata.close(async())
    }, function () {
        assert(1, 'created')
        vivify(tmp, async())
        load(__dirname + '/fixtures/create.after.json', async())
    }, function (actual, expected) {
        assert.say(actual[1])
        assert.say(expected)

        assert(actual, expected, 'written')

        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        assert(cursor.page.items.length - cursor.offset, 0, 'empty')

        cursor.unlock(async())
    }, function () {
        strata.purge(0)
        assert(strata.sheaf.magazine.heft, 0, 'purged')

        strata.close(async())
    })
}
