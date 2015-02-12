#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: __filename })

    strata.create(function (error) {
        assert(/is not a directory.$/.test(error.message), 'thrown')
    })

    var fs = require('fs')
    fs.stat = function (file, callback) { callback(new Error('errored')) }
    // todo: dubious
    strata = createStrata({  directory: tmp })

    strata.create(function (error) {
        assert(error.message, 'errored', 'called back')
    })
}
