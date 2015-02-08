#!/usr/bin/env node

require('./proof')(2, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: __filename })

    strata.create(function (error) {
        assert(/is not a directory.$/.test(error.message), 'thrown')
    })

    // todo: dubious
    strata = createStrata({  directory: tmp, fs: {
        stat: function (file, callback) { callback(new Error('errored')) }
    }})

    strata.create(function (error) {
        assert(error.message, 'errored', 'called back')
    })
}
