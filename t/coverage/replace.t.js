#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    function forward (name) {
        return function () { return fs[name].apply(fs, arguments) }
    }
    var fs = require('fs'), path = require('path'), proxy = {}
    for (var x in fs) {
        if (x[0] != '_') proxy[x] = forward(x)
    }
    proxy.unlink = function (file, callback) {
        var error = new Error()
        error.code = 'EACCES'
        callback(error)
    }
    var strata = new Strata({ directory: tmp, fs: proxy, leafSize: 3 })
    async(function () {
        serialize(__dirname + '/../basics/fixtures/split.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        insert(async, strata, [ 'b' ])
    }, [function () {
        strata.balance(async())
    }, function (_, error) {
        assert(error.code, 'EACCES', 'unlink error')
    }])
})
