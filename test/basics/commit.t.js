require('proof')(1, require('cadence')(prove))

function prove (async, okay) {
    var Commit = require('../../commit')
    var fs = require('fs')
    var path = require('path')
    var utilities = require('../utilities')
    async(function () {
        utilities.reset(utilities.directory, async())
    }, function () {
        fs.writeFile(path.join(utilities.directory, '1.1'), '{}', async())
    }, function () {
        var commit = new Commit(utilities.directory)
        async(function () {
            commit.write(1, [[
                'unlink', path.join(utilities.directory, '1.1')
            ]], async())
        }, function () {
            commit.prepare(async())
        }, function () {
            commit.commit(async())
        }, function () {
            fs.readdir(utilities.directory, async())
        }, function (dir) {
            okay(dir, [ 'commit' ], 'commit performed')
        })
    })
}
