#!/usr/bin/env node

require('./proof')(2, function (Strata, tmp, equal, ok) {
    var strata = new Strata({ directory: __filename })

    strata.create(function (error) {
        ok(/is not a directory.$/.test(error.message), 'thrown')
    })

    strata = new Strata({  directory: tmp, fs: {
        stat: function (file, callback) { callback(new Error('errored')) }
    }})

    strata.create(function (error) {
        equal(error.message, 'errored', 'called back')
    })
})
