var ok = require('assert').ok
var path = require('path')
var fs = require('fs')

var cadence = require('cadence')
var sequester = require('sequester')

var Cache = require('magazine')

var Locker = require('./locker')
var Page = require('./page')

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

function extract (a) { return a }

function Sheaf (options) {
    this.nextAddress = 0
    this.directory = options.directory
    this.cache = options.cache || new Cache()
    this.options = options
    this.tracer = options.tracer || function () { arguments[2]() }
    this.extractor = options.extractor || extract
    this.comparator = options.comparator || compare
    this.player = options.player
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

Sheaf.prototype.createPage = function (modulus, address) {
    return new Page(this, address, modulus)
}

Sheaf.prototype.createMagazine = function () {
    var magazine = this.cache.createMagazine()
    var cartridge = magazine.hold(-2, {
        page: {
            address: -2,
            items: [{ key: null, address: 0, heft: 0 }],
            queue: sequester.createQueue()
        }
    })
    var metaRoot = cartridge.value.page
    metaRoot.cartridge = cartridge
    metaRoot.lock = metaRoot.queue.createLock()
    metaRoot.lock.share(function () {})
    this.metaRoot = metaRoot
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
