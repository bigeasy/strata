#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var fs = require('fs'), path = require('path')
    var strata = new Strata({ directory: tmp, checksum: 'none' })
    step(function () {
        strata.create(step())
    }, function () {
        strata.close(step())
    }, function () {
        fs.readFile(path.join(tmp, '0'), 'utf8', step())
    }, function (body) {
        assert(+(body.split(/\n/)[0].split(/\s+/)[1]), 0, 'zero')
    })
})
