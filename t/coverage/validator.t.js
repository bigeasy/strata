#!/usr/bin/env node

require('./proof')(2, function (step, assert) {
    var strata = new Strata({ directory: __filename })

    strata.create(function (error) {
        assert(/is not a directory.$/.test(error.message), 'thrown')
    })

    strata = new Strata({  directory: tmp, fs: {
        stat: function (file, callback) { callback(new Error('errored')) }
    }})

    strata.create(function (error) {
        assert(error.message, 'errored', 'called back')
    })
})
