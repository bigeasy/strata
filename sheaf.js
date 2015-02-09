var ok = require('assert').ok
var path = require('path')

var cadence = require('cadence/redux')

require('cadence/loops')

var Cache = require('magazine')

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
    var root = this.createPage(0)
    var leaf = this.createPage(1)
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

Sheaf.prototype.heft = function (page, s) {
    this.magazine.get(page.address).adjustHeft(s)
}

function Page (sheaf, address, modulus) {
    if (address == null) {
        while (sheaf.nextAddress % 2 !== modulus) sheaf.nextAddress++
        address = sheaf.nextAddress++
    }
    this.address = address
    this.entries = 0
    this.rotation = 0
    this.items = []
    this.queue = sheaf.sequester.createQueue()
    if (modulus === 1) {
        this.right = { address: 0, key: null }
        this.ghosts = 0
    }
}

Sheaf.prototype.createPage = function (modulus, address) {
    return new Page(this, address, modulus)
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
