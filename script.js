var cadence = require('cadence/redux')
var path = require('path')
var fs = require('fs')
var Queue = require('./queue')

function Script (sheaf) {
    this._sheaf = sheaf
    this._journal = []
    this._operations = []
}

Script.prototype.rotate = function (page) {
    page.position = 0
    page.rotation++
    var queue = new Queue
    this._sheaf.writeHeader(queue, page)
    queue.finish()
    this._operations.push({
        name: '_rotate', page: page, queue: queue
    })
}

Script.prototype.unlink = function (page) {
    var rotations = []
    for (var i = 0; i <= page.rotation; i++) {
        rotations.push(this._sheaf._filename(page.address, i))
    }
    this._journal.push({
        name: '_purge', rotations: rotations
    })
}

Script.prototype.rewriteLeaf = function (page) {
    this._operations.push({
        name: '_rewriteLeaf', page: page
    })
}

Script.prototype.writeBranch = function (page) {
    this._operations.push({
        name: '_writeBranch', page: page
    })
}

Script.prototype._rotate = cadence(function (async, operation) {
    var page = operation.page, queue = operation.queue, entry
    var rotation = this._sheaf.filename2(page, '.replace')
    this._journal.push({
        name: '_replace', from: rotation, to: this._sheaf.filename2(page)
    })
    async(function () {
        entry = this._sheaf.journal.leaf.open(rotation, page.position, page)
        entry.ready(async())
    }, function () {
        page.position += queue.length
        async.forEach(function (buffer) {
            entry.write(buffer, async())
        })(queue.buffers)
    }, function () {
    // todo: scram on failure.
        entry.close('entry', async())
    }, function () {
        return [ rotation ]
    })
})

Script.prototype._writeBranch = cadence(function (async, operation) {
    var page = operation.page
    var file = this._sheaf.filename2(page, '.replace')
    this._journal.push({
        name: '_replace', from: file, to: this._sheaf.filename2(page)
    })
    this._sheaf.writeBranch(page, file, async())
})

Script.prototype._rewriteLeaf = cadence(function (async, operation) {
    var page = operation.page
    this.unlink(page)
    var file = this._sheaf._filename(page.address, 0, '.replace')
    this._journal.push({
        name: '_replace', from: file, to: this._sheaf._filename(page.address, 0)
    })
    this._sheaf.rewriteLeaf(page, '.replace', async())
})

Script.prototype.commit = cadence(function (async) {
    async(function () {
        async.forEach(function (operation) {
            this[operation.name](operation, async())
        })(this._operations)
    }, function () {
        this._journal.push({ name: '_complete' })
        var pending = path.join(this._sheaf.directory, 'journal.pending')
        var comitted = path.join(this._sheaf.directory, 'journal')
        async(function () {
            var script = this._journal.map(function (operation) {
                return JSON.stringify(operation)
            }).join('\n') + '\n'
            fs.writeFile(pending, script, 'utf8', async())
        }, function () {
            fs.rename(pending, comitted, async())
        }, function () {
            this.play(async())
        })
    })
})

Script.prototype.play = cadence(function (async, page) {
    async.forEach(function (operation) {
        this[operation.name](operation, async())
    })(this._journal)
})

Script.prototype._replace = cadence(function (async, operation) {
    async(function () {
        var block = async([function () {
            fs.stat(operation.from, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
            return [ block ]
        }], [function () {
            fs.unlink(operation.to, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
        }], function () {
            fs.rename(operation.from, operation.to, async())
        }, function () {
            return [ block ]
        })()
    }, function () {
        fs.stat(operation.to, async())
    })
})

Script.prototype._complete = cadence(function (async, operation) {
    var journal = path.join(this._sheaf.directory, 'journal')
    async([function () {
        fs.unlink(journal, async())
    }, function (error) {
        if (error.code !== 'ENOENT') {
            throw error
        }
    }])
})

Script.prototype._purge = cadence(function (async, operation) {
    async.forEach(function (file) {
        async([function () {
            fs.unlink(file, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
        }])
    })(operation.rotations)
})

module.exports = Script
