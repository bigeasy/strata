var ok = require('assert').ok
var path = require('path')

var cadence = require('cadence/redux')

require('cadence/loops')

var Journalist = require('journalist')
var Cache = require('magazine')

var extend = require('./extend')

var Locker = require('./locker')
var Queue = require('./queue')
var Script = require('./script')

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

function extract (a) { return a }

function Sheaf (options) {
    this.fs = options.fs || require('fs')
    this.nextAddress = 0
    this.directory = options.directory
    this.journal = {
        branch: new Journalist({ stage: 'entry' }).createJournal(),
        leaf: new Journalist({ stage: 'entry' }).createJournal()
    }
    this.journalist = new Journalist({
        count: options.fileHandleCount || 64,
        stage: options.writeStage || 'leaf',
        cache: options.jouralistCache || new Cache()
    })
    this.cache = options.cache || (new Cache)
    this.options = options
    this.tracer = options.tracer || function () { arguments[2]() }
    this.sequester = options.sequester || require('sequester')
    this.extractor = options.extractor || extract
    this.comparator = options.comparator || compare
    this.player = options.player
    this.checksum = (function () {
        if (typeof options.checksum == 'function') return options.checksum
        var algorithm
        switch (algorithm = options.checksum || 'sha1') {
        case 'none':
            return function () {
                return {
                    update: function () {},
                    digest: function () { return '0' }
                }
            }
        default:
            var crypto = require('crypto')
            return function (m) { return crypto.createHash(algorithm) }
        }
    })()
    this.serialize = options.serialize || function (object) { return new Buffer(JSON.stringify(object)) }
    this.deserialize = options.deserialize || function (buffer) { return JSON.parse(buffer.toString()) }
    this.createJournal = (options.writeStage == 'tree' ? (function () {
        var journal = this.journalist.createJournal()
        return function () { return journal }
    }).call(this) : function () {
        return this.journalist.createJournal()
    })
    this.lengths = {}
}

Sheaf.prototype.create = function () {
    var root = this.createBranch({ penultimate: true })
    var leaf = this.createLeaf()
    this.splice(root, 0, 0, { address: leaf.address, heft: 0 })
    ok(root.address == 0, 'root not zero')
    return { root: root, leaf: leaf }
}

Sheaf.prototype.unbalanced = function (page, force) {
    if (force) {
        this.lengths[page.address] = this.options.leafSize
    } else if (this.lengths[page.address] == null) {
        this.lengths[page.address] = page.items.length - page.ghosts
    }
}

Sheaf.prototype.filename2 = function (page, suffix) {
    return this._filename(page.address, page.rotation, suffix)
}

Sheaf.prototype._filename = function (address, rotation, suffix) {
    suffix || (suffix = '')
    return path.join(this.directory, address + '.' + rotation + suffix)
}

Sheaf.prototype.replace = cadence(function (async, page, suffix) {
    // todo: unlink all rotations
    var replacement = this._filename(page.address, page.rotation, suffix),
        permanent = this._filename(page.address, page.rotation)

    async(function () {
        this.fs.stat(replacement, async())
    }, function (stat) {
        ok(stat.isFile(), 'is not a file')
        async([function () {
            this.fs.unlink(permanent, async())
        }, function (error) {
            if (error.code != 'ENOENT') {
                throw error
            }
        }])
    }, function (ror) {
        this.fs.rename(replacement, permanent, async())
    })
})

Sheaf.prototype._rename = function (page, rotation, from, to, callback) {
    this.fs.rename(
        this._filename(page.address, rotation, from),
        this._filename(page.address, rotation, to),
        callback)
}

Sheaf.prototype._unlink = function (page, rotation, suffix, callback) {
    this.fs.unlink(this._filename(page.address, rotation, suffix), callback)
}

Sheaf.prototype.heft = function (page, s) {
    this.magazine.get(page.address).adjustHeft(s)
}

Sheaf.prototype.createLeaf = function (override) {
    return this.createPage({
        rotation: 0,
        loaders: {},
        entries: 0,
        ghosts: 0,
        items: [],
        right: { address: 0, key: null },
        queue: this.sequester.createQueue()
    }, override, 0)
}

Sheaf.prototype.writeEntry = function (options) {
    var entry, buffer, json, line, length

    ok(options.page.position != null, 'page has not been positioned: ' + options.page.position)
    ok(options.header.every(function (n) { return typeof n == 'number' }), 'header values must be numbers')

    entry = options.header.slice()
    json = JSON.stringify(entry)
    var hash = this.checksum()
    hash.update(json)

    length = 0

    var separator = ''
    if (options.body != null) {
        var body = this.serialize(options.body, options.isKey)
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

Sheaf.prototype.writeInsert = function (queue, page, index, record) {
    var header = [ ++page.entries, index + 1 ]
    return this.writeEntry({ queue: queue, page: page, header: header, body: record, type: 'insert' })
}

Sheaf.prototype.writeDelete = function (queue, page, index, callback) {
    var header = [ ++page.entries, -(index + 1) ]
    this.writeEntry({ queue: queue, page: page, header: header, type: 'delete' })
}

Sheaf.prototype.writeHeader = function (queue, page) {
    var header = [ ++page.entries, 0, page.right.address, page.ghosts || 0 ]
    return this.writeEntry({
        queue: queue, page: page, header: header, isKey: true, body: page.right.key
    })
}

Sheaf.prototype.readFooter = function (entry) {
    var footer = entry.header
    return {
        entry:      footer[0],
        right:      footer[1],
        position:   footer[2],
        entries:    footer[3],
        ghosts:     footer[4],
        records:    footer[5]
    }
}

Sheaf.prototype.rewriteLeaf = cadence(function (async, page, suffix) {
    var index = 0, out

    async(function () {
        out = this.journal.leaf.open(this._filename(page.address, 0, suffix), 0, page)
        out.ready(async())
    }, [function () {
        // todo: ensure that cadence finalizers are registered in order.
        // todo: also, don't you want to use a specific finalizer above?
        // todo: need an error close!
        out.scram(async())
    }], function () {
        page.rotation = 0
        page.position = 0
        page.entries = 0

        var items = this.splice(page, 0, page.items.length)

        var queue = new Queue

        var i = 0, I = items.length
        var loop = async(function () {
            this.writeHeader(queue, page)
        }, function () {
            for (; i < I && queue.buffers.length == 0; i++) {
                var item = items[i]
                this.writeInsert(queue, page, i, item.record)
                this.splice(page, page.items.length, 0, item)
            }
            if (i == I) {
                queue.finish()
            }
            page.position += queue.length
            async.forEach(function (buffer) {
                out.write(buffer, async())
            })(queue.buffers)
        }, function () {
            if (i == I) {
                return [ loop ]
            }
        })()
    }, function () {
        out.close('entry', async())
    })
})

Sheaf.prototype.createPage = function (page, override, remainder) {
    if (override.address == null) {
        while ((this.nextAddress % 2) == remainder) this.nextAddress++
        override.address = this.nextAddress++
    }
    return extend(page, override)
}

Sheaf.prototype.createBranch = function (override) {
    return this.createPage({
        items: [],
        entries: 0,
        rotation: 0,
        penultimate: true,
        queue: this.sequester.createQueue()
    }, override, 1)
}

Sheaf.prototype.splice = function (page, offset, length, insert) {
    ok(typeof page != 'string', 'page is string')
    var items = page.items, heft, removals

    if (length) {
        removals = items.splice(offset, length)
        heft = removals.reduce(function (heft, item) { return heft + item.heft }, 0)
        this.heft(page, -heft)
    } else {
        removals = []
    }

    if (insert != null) {
        if (! Array.isArray(insert)) insert = [ insert ]
        heft = insert.reduce(function (heft, item) { return heft + item.heft }, 0)
        this.heft(page, heft)
        items.splice.apply(items, [ offset, 0 ].concat(insert))
    }
    return removals
}

Sheaf.prototype.writeBranch = cadence(function (async, page, file) {
    var items = page.items, out

    ok(items[0].key == null, 'key of first item must be null')
    ok(items[0].heft == 0, 'heft of first item must be zero')
    ok(items.slice(1).every(function (item) { return item.key != null }), 'null keys')

    var queue = new Queue

    async(function () {
        page.entries = 0
        page.position = 0

        out = this.journal.branch.open(file, 0, page)
        out.ready(async())
    }, [function () {
        out.scram(async())
    }], function () {
        var i = 0, I = page.items.length
        var loop = async(function (item) {
            queue.clear()
            for (; i < I && queue.buffers.length == 0; i++) {
                var item = page.items[i]
                var key = page.entries ? item.key : null
                page.entries++
                var header = [ page.entries, page.entries, item.address ]
                this.writeEntry({
                    queue: queue,
                    page: page,
                    header: header,
                    body: key,
                    isKey: true
                })
            }
            if (i == I) {
                queue.finish()
            }
            page.position += queue.length
            async.forEach(function (buffer) {
                out.write(buffer, async())
            })(queue.buffers)
        }, function () {
            if (i == I) {
                return [ loop ]
            }
        })()
    }, function () {
        out.close('entry', async())
    })
})

Sheaf.prototype.createMagazine = function () {
    var magazine = this.cache.createMagazine()
    var dummy = magazine.hold(-2, {
        page: {
            address: -2,
            items: [{ key: null, address: 0, heft: 0 }],
            queue: this.sequester.createQueue()
        }
    }).value.page
    dummy.lock = dummy.queue.createLock()
    dummy.lock.share(function () {})
    this.magazine = magazine
}

Sheaf.prototype.createLocker = function () {
    return new Locker(this, this.magazine)
}

Sheaf.prototype.find = function (page, key, low) {
    var mid, high = page.items.length - 1

    while (low <= high) {
        mid = low + ((high - low) >>> 1)
        var compare = this.comparator(key, page.items[mid].key)
        if (compare < 0) high = mid - 1
        else if (compare > 0) low = mid + 1
        else return mid
    }

    return ~low
}

module.exports = Sheaf
