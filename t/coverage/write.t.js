require('./proof')(1, prove)

function prove (async, okay) {
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

    var strata = createStrata({ directory: tmp, fs: proxy, leafSize: 3 })
    async(function () {
        serialize(__dirname + '/../basics/fixtures/split.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        cursor.insert('b', 'b', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/write.after.json', async())
    }, function (actual, expected) {
        okay.say(actual)
        okay.say(expected)
        okay(actual, expected, 'written')
    })
}
