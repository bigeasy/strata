#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var fs = require('fs'), crypto = require('crypto'), strata
    async(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            checksum: function () { return crypto.createHash('sha1') }
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        assert(cursor.length - cursor.offset, 0, 'empty')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
