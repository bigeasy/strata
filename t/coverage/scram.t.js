require('./proof')(2, prove)

function prove (async, okay) {
    var scram = require('../../scram'), entry = {
        scram: function (callback) {
            okay('called')
            callback()
        }
    }
    scram(entry, function (callback) {
        callback(new Error('abend'))
    }, function (error) {
        okay(error.message, 'abend', 'error thrown')
    })
}
