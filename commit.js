var fnv = require('hash.fnv')
var cadence = require('cadence')
var fs = require('fs')
var path = require('path')
var mkdirp = require('mkdirp')
var assert = require('assert')
var rimraf = require('rimraf')

function Commit (directory) {
    this.directory = directory
}

Commit.prototype.write = cadence(function (async, instance, commit) {
    async(function () {
        this._readdir(async())
    }, function (dir) {
        assert.deepStrictEqual(dir, [], 'commit directory not empty')
        var adorned = commit.map(function (operation) {
            switch (operation[0]) {
            case 'unlink':
                return [ operation ]
            }
        })
        adorned.unshift([[ 'begin' ]])
        adorned.push([[ 'end' ]])
        this._write('commit', adorned, async())
    })
})

Commit.prototype._write = cadence(function (async, file, entries) {
    var directory = path.join(this.directory, 'commit')
    var buffer = Buffer.from(entries.map(JSON.stringify).join('\n'))
    async(function () {
        fs.writeFile(path.join(directory, 'prepare'), buffer, async())
    }, function () {
        var hash = Number(fnv(buffer, 0, buffer.length, 0)).toString(16)
        var from = path.join(directory, 'prepare')
        var to = path.join(directory, file + '-' + hash)
        fs.rename(from, to, async())
    })
})

Commit.prototype._load = cadence(function (async, file) {
    async(function () {
        fs.readFile(path.join(this.directory, 'commit', file), async())
    }, function (buffer) {
        var hash = Number(fnv(buffer, 0, buffer.length, 0)).toString(16)
        assert.equal(hash, file.split('-')[1], 'commit hash failure')
        return [ buffer.toString().split('\n').filter(function (line) {
            return line != ''
        }).map(JSON.parse) ]
    })
})

Commit.prototype._readdir = cadence(function (async) {
    var directory = path.join(this.directory, 'commit')
    async(function () {
        mkdirp(directory, async())
    }, function () {
        fs.readdir(directory, async())
    }, function (dir) {
        return [ dir.filter(function (file) { return ! /^\./.test(file) }) ]
    })
})

Commit.prototype.prepare = cadence(function (async, stop) {
    var directory = path.join(this.directory, 'commit')
    async(function () {
        this._readdir(async())
    }, function (dir) {
        var commit = dir.filter(function (file) {
            return /^commit-/.test(file)
        }).shift()
        if (commit == null) {
            return [ async.break, false ]
        }
        async(function () {
            var other = dir.filter(function (file) { return file != commit })
            async.forEach([ other ], function () {
                fs.unlink(path.join(directory, other), async())
            })
        }, function () {
            this._load(commit, async())
        }, function (operations) {
            var index = 0
            async.forEach([ operations ], function (steps) {
                if (index == stop) {
                    return [ async.break ]
                }
                async.forEach([ steps ], function (step) {
                    this._write(String(index++), [ step ], async())
                })
            })
        }, function () {
            return true
        })
    })
})

Commit.prototype.commit = cadence(function (async) {
    var directory = path.join(this.directory, 'commit')
    async(function () {
        this._readdir(async())
    }, function (dir) {
        var operations = dir.filter(function (file) {
            return /^\d+-[0-9a-f]{8}$/.test(file)
        }).map(function (file) {
            var split = file.split('-')
            return { index: +split[0], file: file, hash: split[1] }
        }).sort(function (left, right) {
            return left.index - right.index
        })
        async.forEach([ operations ], function (operation) {
            async(function () {
                this._load(operation.file, async())
            }, function (entries) {
                var operation = entries.shift()
                switch (operation.shift()) {
                case 'begin':
                    var commit = dir.filter(function (file) {
                        return /^commit-/.test(file)
                    }).shift()
                    fs.unlink(path.join(directory, commit), async())
                    break
                case 'unlink':
                    rimraf(operation.shift(), async())
                    break
                case 'end':
                    break
                }
            }, function () {
                fs.unlink(path.join(directory, operation.file), async())
            })
        })
    })
})

module.exports = Commit
