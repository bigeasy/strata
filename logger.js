var ok = require('assert').ok

var cadence = require('cadence/redux')

var Queue = require('./queue')
var Scribe = require('./scribe')

function Logger (sheaf) {
    // todo: remove when page can slice
    this._sheaf = sheaf
}

Logger.prototype.writeEntry = function (options) {
    var entry, buffer, json, line, length

    ok(options.page.position != null, 'page has not been positioned: ' + options.page.position)
    ok(options.header.every(function (n) { return typeof n == 'number' }), 'header values must be numbers')

    entry = options.header.slice()
    json = JSON.stringify(entry)
    var hash = this._sheaf.checksum()
    hash.update(json)

    length = 0

    var separator = ''
    if (options.body != null) {
        var body = this._sheaf.serialize(options.body, options.isKey)
        separator = ' '
        length += body.length
        hash.update(body)
    }

    line = hash.digest('hex') + ' ' + json + separator

    length += Buffer.byteLength(line, 'utf8') + 1

    var entire = length + String(length).length + 1
    if (entire < length + String(entire).length + 1) {
        length = length + String(entire).length + 1
    } else {
        length = entire
    }

    buffer = options.queue.slice(length)

    buffer.write(String(length) + ' ' + line)
    if (options.body != null) {
        body.copy(buffer, buffer.length - 1 - body.length)
    }
    buffer[length - 1] = 0x0A

    return length
}

Logger.prototype.writeInsert = function (queue, page, index, record) {
    var header = [ ++page.entries, index + 1 ]
    return this.writeEntry({ queue: queue, page: page, header: header, body: record, type: 'insert' })
}

Logger.prototype.writeDelete = function (queue, page, index, callback) {
    var header = [ ++page.entries, -(index + 1) ]
    this.writeEntry({ queue: queue, page: page, header: header, type: 'delete' })
}

Logger.prototype.writeHeader = function (queue, page) {
    var header = [ ++page.entries, 0, page.right.address, page.ghosts || 0 ]
    return this.writeEntry({
        queue: queue, page: page, header: header, isKey: true, body: page.right.key
    })
}

Logger.prototype.writeLeafEntry = function (queue, page, item) {
    this.writeInsert(queue, page, page.entries - 1, item.record)
}

Logger.prototype.rewriteLeaf = function (page, suffix, callback) {
    this.writePage(page, this._sheaf._filename(page.address, 0, suffix), 'writeLeafEntry', callback)
}

Logger.prototype.writeBranchEntry = function (queue, page, item) {
    page.entries++
    var header = [ page.entries, page.entries, item.address ]
    this.writeEntry({
        queue: queue,
        page: page,
        header: header,
        body: page.entries == 1 ? null : item.key,
        isKey: true
    })
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

    page.rotation = 0
    page.entries = 0
    page.position = 0

    var leaf = page.address % 2 === 1
    if (leaf) {
        this.writeHeader(queue, page)
    }

    var i = 0, I = page.items.length
    while (i < I) {
        for (; i < I && queue.buffers.length == 0; i++) {
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

Logger.prototype.rotate = cadence(function (async, page) {
    page.position = 0
    page.rotation++

    var from = this._sheaf.filename2(page, '.replace')
    var to = this._sheaf.filename2(page)

    var scribe = new Scribe(from, 'a')
    scribe.open()

    var queue = new Queue
    this.writeHeader(queue, page)
    queue.finish()
    queue.buffers.forEach(function (buffer) {
        scribe.write(buffer, 0, buffer.length, page.position)
        page.position += buffer.length
    })

    async(function () {
        scribe.close(async())
    }, function () {
        return [ from, to ]
    })
})

module.exports = Logger
