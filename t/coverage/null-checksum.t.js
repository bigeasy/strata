#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var fs = require('fs'), path = require('path')
    var strata = new Strata({ directory: tmp, checksum: 'none' })
    async(function () {
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        fs.readFile(path.join(tmp, '0'), 'utf8', async())
    }, function (body) {
        assert(+(body.split(/\n/)[0].split(/\s+/)[1]), 0, 'zero')
    })
})
