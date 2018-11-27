require('./proof')(2, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: __filename })

    strata.create(function (error) {
        okay(/is not a directory.$/.test(error.message), 'thrown')
    })

    var fs = require('fs')
    fs.stat = function (file, callback) { callback(new Error('errored')) }
    // todo: dubious
    strata = createStrata({  directory: tmp })

    strata.create(function (error) {
        okay(error.message, 'errored', 'called back')
    })
}
