var fnv = require('hash.fnv')
var cadence = require('cadence')
var fs = require('fs')
var path = require('path')
var mkdirp = require('mkdirp')
var assert = require('assert')
var rimraf = require('rimraf')

function Commit (journalist) {
    this._jouralist = journalist
    this._index = 0
}

Commit.prototype.write = cadence(function (async, instance, commit) {
    async(function () {
        this._readdir(async())
    }, function (dir) {
        assert.deepStrictEqual(dir, [], 'commit directory not empty')
        this._write('commit', commit, async())
    })
})

Commit.prototype._prepare = cadence(function (async, operation) {
    this._write(String(this._index++), [ operation ], async())
})

Commit.prototype._write = cadence(function (async, file, entries) {
    var directory = path.join(this._jouralist.directory, 'commit')
    var buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
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
        fs.readFile(path.join(this._jouralist.directory, 'commit', file), async())
    }, function (buffer) {
        var hash = Number(fnv(buffer, 0, buffer.length, 0)).toString(16)
        assert.equal(hash, file.split('-')[1], 'commit hash failure')
        return [ file, buffer.toString().split('\n').filter(function (line) {
            return line != ''
        }).map(JSON.parse) ]
    })
})

Commit.prototype._readdir = cadence(function (async) {
    var directory = path.join(this._jouralist.directory, 'commit')
    async(function () {
        mkdirp(directory, async())
    }, function () {
        fs.readdir(directory, async())
    }, function (dir) {
        return [ dir.filter(function (file) { return ! /^\./.test(file) }) ]
    })
})

Commit.prototype._emplace = cadence(function (async, pages) {
    async.forEach([ pages ], function (page) {
        async(function () {
            this._journalist.write(page, 'commit', async())
        }, function (write) {
            async(function () {
                this._journalist.load(page.id, async())
            }, function (cartridge) {
                cartridge.heft = write.size
                cartridge.release()
                fs.readFile(this._journalist.path('commit', write.append), async())
            }, function (buffer) {
                var hash = fnv(buffer, 0, buffer.length, 0)
                var from = write.append
                var to = path.join('pages', write.page.id)
                this._prepare([ 'move', from, to, hash ], async())
            }, function () {
                var rm = path.join('pages', write.page.id, write.page.append)
                this._prepare([ 'unlink', rm ], async())
            })
        })
    })
})

Commit.prototype.prepare = cadence(function (async, stop) {
    var directory = path.join(this._jouralist.directory, 'commit')
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
            return commit
        })
    }, function (commit) {
        this._load(commit, async())
    }, function (commit, operations) {
        operations.unshift([ 'begin' ])
        operations.push([ 'end' ])
        async.forEach([ operations ], function (operation, step) {
            switch (operation[0]) {
            case 'begin':
                this._prepare([ 'unlink', path.join('commit', commit) ], async())
                break
            case 'split':
                async(function () {
                    this._journalist.read(operation[1], async())
                }, function (page) {
                    var right = {
                        id: this._journalist.createId(),
                        items: page.items.splice(operation[2])
                    }
                    this._emplace([ page, right ], async())
                })
                break
            case 'splice':
                async(function () {
                    this._journalist.read(operation[1], async())
                }, function (page) {
                    page.items.splice.apply(page.items, operation.vargs)
                    this._emplace([ page ], async())
                })
                break
            case 'drain':
                async(function () {
                    this._journalist.read('0.0', async())
                }, function (root) {
                    var right = {
                        id: this._journalist.createId(),
                        items: root.splice(operation[1])
                    }
                    var left = {
                        id: this._journalist.createId(),
                        items: root.splice(0)
                    }
                    root.items = [{
                        id: left.id,
                        key: null
                    }, {
                        id: right.id,
                        key: right.items[0].key
                    }]
                    right.items[0].key = null
                    this._emplace([ root, left, right ], async())
                })
                break
            case 'fill':
                async(function () {
                    this._journalist.read('0.0', async())
                }, function (root) {
                    async(function () {
                        this._journalist.read(items[0].id, async())
                    }, function (page) {
                        root.items = page.items
                        async(function () {
                            this._emplace([ root ], async())
                        }, function () {
                            this._prepare([ 'unlink', path.join('pages', page.id) ], async())
                        })
                    })
                })
                break
            case 'unlink':
                this._prepare(operation, async())
                break
            case 'end':
                this._prepare([ 'end' ], async())
                break
            }
        })
    }, function () {
        return true
    })
})

Commit.prototype.commit = cadence(function (async) {
    var directory = path.join(this._jouralist.directory, 'commit')
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
            }, function (file, entries) {
                var operation = entries.shift()
                switch (operation.shift()) {
                case 'begin':
                    var commit = dir.filter(function (file) {
                        return /^commit-/.test(file)
                    }).shift()
                    fs.unlink(path.join(directory, commit), async())
                    break
                case 'move':
                    var from = operation.shift()
                    var to = operation.shift()
                    async(function () {
                        fs.rename(from, to, async())
                    }, function () {
                        fs.readFile(to, async())
                    }, function (buffer) {
                        var hash = fnv(buffer, 0, buffer.length, 0)
                        assert.equal(hash, operation.shift(), 'move failed')
                    })
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
