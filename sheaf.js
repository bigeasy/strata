var ok = require('assert').ok
var path = require('path')
var fs = require('fs')

var mkdirp = require('mkdirp')

var Staccato = require('staccato')

var Signal = require('signal')

var Turnstile = require('turnstile')
Turnstile.Set = require('turnstile/set')

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
    this._checksum = function () { return "0" }
    this.tracer = options.tracer || function () { arguments[2]() }
    this.extractor = options.extractor || extract
    this.comparator = options.comparator || compare
    this.player = options.player
    this.lengths = {}
    this.turnstile = new Turnstile
    this._lock = new Turnstile.Set(this, '_locked', this.turnstile)
    this._queues = {}
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

Sheaf.prototype.hold = function (id) {
    return this._sheaf.hold(id, null)
}

Sheaf.prototype._operate = cadence(function (async, entry) {
})

// TODO Okay, I'm getting tired of having to check canceled and unit test for
// it, so let's have exploding turnstiles (or just let them OOM?) Maybe on
// timeout we crash?
//
// We can ignore canceled here, I believe, and just work through anything left,
// but we should document this as a valid attitude to work in Turnstile.
//
// Writing things out again. Didn't occur to me
Sheaf.prototype._locked = cadence(function (async, envelope) {
    var queue = this._queues[envelope.body], entry
    console.log('here')
    async(function () {
        async.loop([], function () {
            if (queue.length == 0) {
                return [ async.break ]
            }
            var entry = queue.pop()
            async(function () {
                switch (entry.method) {
                case 'write':
                    var directory = path.resolve(this.directory, String(envelope.body))
                    async(function () {
                        mkdirp(directory, async())
                    }, function () {
                        var stream = fs.createWriteStream(path.resolve(directory, 'append'), { flags: 'a' })
                        var writable = new Staccato.Writable(stream)
                        async(function () {
                            async.forEach([ entry.writes ], function (write) {
                                var header = Buffer.from(JSON.stringify({
                                    position: write.position,
                                    previous: write.previous,
                                    method: write.method,
                                    index: write.index,
                                    length: write.serialized.length
                                }) + '\n')
                                var record = Buffer.concat([ header, write.serialized ])
                                var checksum = JSON.stringify(this._checksum.call(null, record, 0, record.length)) + '\n'
                                async(function () {
                                    writable.write(checksum, async())
                                }, function () {
                                    writable.write(record, async())
                                })
                            })
                        }, function () {
                            writable.end(async())
                        }, function () {
                            stream.close(async())
                        })
                    })
                    break
                }
            }, function () {
                entry.completed.unlatch()
            })
        })
    })
})

Sheaf.prototype.append = function (entry) {
    var queue = this._queues[entry.id]
    if (queue == null) {
        var queue = this._queues[entry.id] = [{ method: 'write', writes: [], completed: new Signal }]
    }
    queue[0].writes.push(entry)
    this._lock.add(entry.id)
    return queue.signal
}

module.exports = Sheaf
