#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var path = require('path')
    script({
        file: path.join(__dirname, 'fixtures', 'unpurged-key.txt'),
        directory: tmp,
        cadence: require('cadence'),
        assert: assert
    }, async())
})
