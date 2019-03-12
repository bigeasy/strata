// todo: redux
require('proof')(3, require('cadence')(prove))

function prove (async, okay) {
    var path = require('path')
    var rimraf = require('rimraf')
    var mkdirp = require('mkdirp')
    var fs = require('fs')
    var Scribe = require('../../scribe')

    var directory = path.join(__dirname, 'tmp')
    var file = path.join(directory, 'file')
    async(function () {
        rimraf(directory, async())
    }, function () {
        mkdirp(directory, async())
    }, function () {
        var scribe = new Scribe(file, 'w+')
        scribe.open()
        async(function () {
            scribe.write(new Buffer('x'), 0, 1, 0)
            scribe.write(new Buffer('y'), 0, 1, 1, async())
        }, function () {
            setImmediate(async())
        }, function () {
            scribe.write(new Buffer('z'), 0, 1, 2)
            scribe.close(async())
        })
    }, function () {
        fs.readFile(file, 'utf8', async())
    }, function (body) {
        okay(body, 'xyz', 'write')
        async([function () {
            var scribe = new Scribe(path.join(__dirname, 'missing', 'file'), 'w+')
            scribe.open()
            scribe.write(new Buffer('x'), 0, 1, 0)
            scribe.close(async())
        }, function (error) {
            okay(error.code, 'ENOENT', 'immediate error')
        }])
    }, function () {
        fs.open(file, 'r', async())
    }, function (fd) {
        async([function () {
            var scribe = new Scribe(path.join(directory, 'write'), 'w+')
            scribe.open()
            async(function () {
                scribe.write(new Buffer('y'), 0, 1, 0, async())
            }, function () {
                scribe.fd = fd
                scribe.write(new Buffer('y'), 0, 1, 1)
                setTimeout(async(), 500)
            }, function () {
                scribe.write(new Buffer('z'), 0, 1, 2)
                scribe.close(async())
            })
        }, function (error) {
            okay(error.code, 'EBADF', 'later error')
        }])
    }, function () {
        rimraf(directory, async())
    })
}
