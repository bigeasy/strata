var ok = require('assert').ok
var path = require('path')

var cadence = require('cadence/redux')

require('cadence/loops')

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
    this.cache = options.cache || (new Cache)
    this.options = options
    this.tracer = options.tracer || function () { arguments[2]() }
    this.sequester = options.sequester || require('sequester')
    this.extractor = options.extractor || extract
    this.comparator = options.comparator || compare
    this.player = options.player
    this.logger = options.logger
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
