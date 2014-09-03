#!/usr/bin/env node

require('./proof')(2, function (step, assert) {
    var scram = require('../../scram'), entry = {
        scram: function (callback) {
            assert(1, 'called')
            callback()
        }
    }
    scram(entry, function (callback) {
        callback(new Error('abend'))
    }, function (error) {
        assert(error.message, 'abend', 'error thrown')
    })
})
