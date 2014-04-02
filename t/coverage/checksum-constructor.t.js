#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, load, objectify, equal, deepEqual, ok, say) {
    var fs = require('fs'), crypto = require('crypto'), strata
    step(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            checksum: function () { return crypto.createHash('sha1') }
        })
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        equal(cursor.length - cursor.offset, 0, 'empty')
        cursor.unlock()
        strata.close(step())
    })
})
