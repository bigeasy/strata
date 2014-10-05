#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    function forward (name) { return function () { return fs[name].apply(fs, arguments) } }

    var fs = require('fs'), path = require('path'), proxy = {}
    for (var x in fs) {
        if (x[0] != '_') proxy[x] = forward(x)
    }

    var count = 0
    proxy.write = function () {
        if (arguments[3] > 10) {
            arguments[3] = 10
            fs.write.apply(fs, arguments)
        } else {
            fs.write.apply(fs, arguments)
        }
    }

    var strata = new Strata({ directory: tmp, fs: proxy, leafSize: 3 })
    step(function () {
        serialize(__dirname + '/../basics/fixtures/split.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        insert(step, strata, [ 'b' ])
    }, function () {
        strata.close(step())
    }, function () {
        vivify(tmp, step())
        load(__dirname + '/fixtures/write.after.json', step())
    }, function (actual, expected) {
        assert.say(actual)
        assert.say(expected)
        assert(actual, expected, 'written')
    })
})
