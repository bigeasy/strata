var cadence = require('cadence')
var path = require('path')
var fs = require('fs')
var Queue = require('./queue')

function Script (logger) {
    this._logger = logger
    this._journal = []
    this._operations = []
}

Script.prototype.rotate = function (page) {
    this._operations.push({ name: '_rotate', page: page })
}

Script.prototype.unlink = function (page) {
    var rotations = []
    for (var i = 0; i <= page.rotation; i++) {
        rotations.push(path.join(this._logger._directory, 'pages', page.address + '.' + i))
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

Script.prototype._rotate = function (operation, callback) {
    var page = operation.page, queue = operation.queue, entry
    page.rotation++
    var from = this._logger.filename(page, true)
    var to = this._logger.filename(page)
    this._journal.push({
        name: '_replace', from: from, to: to
    })
    this._logger.rotate(page, from, callback)
}

Script.prototype._writeBranch = cadence(function (async, operation) {
    var page = operation.page
    var file = this._logger.filename(page, true)
    this._journal.push({
        name: '_replace', from: file, to: this._logger.filename(page)
    })
    this._logger.writeBranch(page, file, async())
})

Script.prototype._rewriteLeaf = cadence(function (async, operation) {
    var page = operation.page
    this.unlink(page)
    page.rotation = 0
    var file = this._logger.filename(page, true)
    this._journal.push({
        name: '_replace', from: file, to: this._logger.filename(page)
    })
    this._logger.rewriteLeaf(page, file, async())
})

Script.prototype.commit = cadence(function (async) {
    async(function () {
        async.forEach([ this._operations ], function (operation) {
            this[operation.name](operation, async())
        })
    }, function () {
        this._journal.push({ name: '_complete' })
        var pending = path.join(this._logger._directory, 'journal.pending')
        var comitted = path.join(this._logger._directory, 'journal')
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
    async.forEach([ this._journal ], function (operation) {
        this[operation.name](operation, async())
    })
})

Script.prototype._replace = cadence(function (async, operation) {
    async(function () {
        async([function () {
            fs.stat(operation.from, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
            return [ async.return ]
        }], [function () {
            fs.unlink(operation.to, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
        }], function () {
            fs.rename(operation.from, operation.to, async())
        })
    }, function () {
        fs.stat(operation.to, async())
    })
})

Script.prototype._complete = cadence(function (async, operation) {
    var journal = path.join(this._logger._directory, 'journal')
    async([function () {
        fs.unlink(journal, async())
    }, function (error) {
        if (error.code !== 'ENOENT') {
            throw error
        }
    }])
})

Script.prototype._purge = cadence(function (async, operation) {
    async.forEach([ operation.rotations], function (file) {
        async([function () {
            fs.unlink(file, async())
        }, function (error) {
            if (error.code !== 'ENOENT') {
                throw error
            }
        }])
    })
})

module.exports = Script
