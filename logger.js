var ok = require('assert').ok

var fs = require('fs')
var path = require('path')

var cadence = require('cadence')

var Queue = require('./queue')
var Script = require('./script')
var Scribe = require('./scribe')

function Logger (options) {
    this._directory = options.directory
    // TODO remove when page can slice
    this._sheaf = options.sheaf
    this.framer = options.framer
    this.serializers = options.serializers
}

Logger.prototype.filename = function (page, draft) {
    return path.join(this._directory, draft ? 'drafts' : 'pages', page.address + '.' + page.rotation)
}

Logger.prototype.writeEntry = function (queue, header, body, serializer) {
    return this.framer.serialize(queue, header, body, serializer)
}

Logger.prototype.writeInsert = function (queue, page, index, record) {
    var header = [ ++page.entries, index + 1 ]
    return this.writeEntry(queue, header, record, this.serializers.record)
}

Logger.prototype.writeDelete = function (queue, page, index, callback) {
    var header = [ ++page.entries, -(index + 1) ]
    this.writeEntry(queue, header)
}

Logger.prototype.writeHeader = function (queue, page) {
    var header = [ -(++page.entries), page.right.address || 0, page.ghosts || 0 ]
    return this.writeEntry(queue, header, page.right.key, this.serializers.key)
}

Logger.prototype.writeLeafEntry = function (queue, page, item) {
    this.writeInsert(queue, page, page.entries - 1, item.record, this.serializers.record)
}

Logger.prototype.rewriteLeaf = function (page, file, callback) {
    this.writePage(page, file, 'writeLeafEntry', callback)
}

Logger.prototype.writeBranchEntry = function (queue, page, item) {
    page.entries++
    var header = [ page.entries, page.entries, item.address ]
    this.writeEntry(queue, header, page.entries == 1 ? null : item.key, this.serializers.key)
}

Logger.prototype.writeBranch = function (page, file, callback) {
    var items = page.items
    ok(items[0].key == null, 'key of first item must be null')
    ok(items[0].heft == 0, 'heft of first item must be zero')
    ok(items.slice(1).every(function (item) { return item.key != null }), 'null keys')
    this.writePage(page, file, 'writeBranchEntry', callback)
}

Logger.prototype.writePage = function (page, file, writer, callback) {
    var items = page.items, out

    var queue = new Queue
    var scribe = new Scribe(file, 'a')

    scribe.open()

    page.entries = 0
    page.position = 0

    var leaf = page.address % 2 === 1
    if (leaf) {
        this.writeHeader(queue, page)
    }

    // cut the items out because some might be recently promoted keys that have
    // zero heft, we calculate heft here and now.
    var cut = page.splice(0, page.items.length), i = 0, I = cut.length
    while (i < I) {
        for (; i < I && queue.buffers.length == 0; i++) {
            page.splice(i, 0, cut[i])
            this[writer](queue, page, page.items[i])
        }
        queue.buffers.forEach(function (buffer) {
            scribe.write(buffer, 0, buffer.length, page.position)
            page.position += buffer.length
        })
        queue.clear()
    }

    queue.finish()
    queue.buffers.forEach(function (buffer) {
        scribe.write(buffer, 0, buffer.length, page.position)
        page.position += buffer.length
    })

    scribe.close(callback)
}

Logger.prototype.rotate = function (page, file, callback) {
    page.position = 0

    var scribe = new Scribe(file, 'a')
    scribe.open()

    var queue = new Queue
    this.writeHeader(queue, page)
    queue.finish()
    queue.buffers.forEach(function (buffer) {
        scribe.write(buffer, 0, buffer.length, page.position)
        page.position += buffer.length
    })

    scribe.close(callback)
}

Logger.prototype.mkdir = cadence(function (async) {
    async([function () {
        fs.mkdir(path.join(this._directory, 'pages'), 0755, async())
        fs.mkdir(path.join(this._directory, 'drafts'), 0755, async())
    }, function (error) {
        throw error
    }])
})

Logger.prototype.createScript = function () {
    return new Script(this)
}

Logger.prototype.createAppender = function (page) {
    var scribe = new Scribe(this.filename(page), 'a')
    scribe.open()
    return new Appender(this, scribe, page)
}

function Appender (logger, scribe, page) {
    this._logger = logger
    this._scribe = scribe
    this._page = page
    this._queue = new Queue
}

Appender.prototype.writeInsert = function (index, record) {
    var length = this._logger.writeInsert(this._queue, this._page, index, record)
    this._write()
    return length
}

Appender.prototype.writeDelete = function (index) {
    var length = this._logger.writeDelete(this._queue, this._page, index)
    this._write()
    return length
}

Appender.prototype.writeUserRecord = function (header, body) {
    header = [ -(++this._page.entries) ].concat(header)
    var length = this._logger.writeEntry(this._queue, header, body, this._logger.serializers.record)
    this._write()
    return length
}

Appender.prototype._write = function () {
    if (this._queue.buffers.length) {
        this._queue.buffers.forEach(function (buffer) {
            this._scribe.write(buffer, 0, buffer.length, this._page.position)
            this._page.position += buffer.length
        }, this)
        this._queue.clear()
    }
}

Appender.prototype.close = function (callback) {
    this._queue.finish()
    this._write()
    this._scribe.close(callback)
}

module.exports = Logger
