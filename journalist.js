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

var recorder = require('./recorder')(function () { return '0' })

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

function extract (a) { return a }

var Interrupt = require('interrupt').createInterrupter('b-tree')

function Journalist (options) {
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

Journalist.prototype.create = function () {
    var root = this.createPage(0)
    var leaf = this.createPage(1)
    this.splice(root, 0, 0, { address: leaf.address, heft: 0 })
    ok(root.address == 0, 'root not zero')
    return { root: root, leaf: leaf }
}

Journalist.prototype.unbalanced = function (page, force) {
    if (force) {
        this.lengths[page.address] = this.options.leafSize
    } else if (this.lengths[page.address] == null) {
        this.lengths[page.address] = page.items.length - page.ghosts
    }
}

Journalist.prototype.createPage = function (modulus, address) {
    return new Page(this, address, modulus)
}

Journalist.prototype.createMagazine = function () {
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

Journalist.prototype.createLocker = function () {
    return new Locker(this, this.magazine)
}

function Appender (file) {
    this.writable = new Staccato.Writable(file, { flags: 'a' })
}

Appender.prototype.append = cadence(function (async, header, body) {
    this.writable.write(recorder(header, body), async())
})

Appender.prototype.end = cadence(function (async) {
    this.writable.end(async())
})

Journalist.prototype.commit = cadence(function (async, script) {
    var directory = path.resolve(hthis.directory, 'transaction')
    async(function () {
        mkdirp(directory, async())
    }, function () {
        var appender = new Appender(path.resolve(directory, 'prepare'))
        async(function () {
            async.forEach([ entry.writes ], function (write) {
                appender.append(entry, async())
            })
        }, function () {
            appender.end(async())
        })
    }, function () {
        fs.rename(path.resolve(directory, 'prepare'), path.resolve(directory, 'commit'), async())
    }, function () {
        this.transact(async())
    })
})

Journalist.prototype.transact = cadence(function (async, script) {
    var directory = path.resolve(hthis.directory, 'transaction')
    async(function () {
        fs.readFile(path.resolve(this.directory, 'transaction', 'prepare'), 'utf8', async())
    }, function () {
        entries = entries.split(/\n/)
        entries.pop()
        entries = entries.map(function (entry) { return JSON.parse(entry) })
        async.loop([], function () {
            if (entries.length == 0) {
                return [ async.break ]
            }
            var record = shifter(entries), header = record[0]
            switch (header.method) {
            case 'unlink':
                async([function () {
                    fs.unlink(path.resolve(this.directory, header.path), async())
                }, rescue(/^code:ENOENT$/)])
                break
            case 'rename':
                async([function () {
                    fs.rename(path.resolve(this.directory, header.from), path.resolve(this.directory, header.to), async())
                }, rescue(/^code:ENOENT$/)])
                break
            }
        })
    }, function () {
        async([function () {
            fs.unlink(path.resolve(this.directory, 'transaction', 'commit'), async())
        }, rescue(/^code:ENOENT$/)])
    }, function () {
        fs.readdir(path.resolve(this.directory, 'transaction'), async())
    }, function (files) {
        Interrupt.assert(files.filter(function (file) {
            return ! /^\./.test(file)
        }).length == 0, 'interrupt')
    })
})

// TODO Okay, I'm getting tired of having to check canceled and unit test for
// it, so let's have exploding turnstiles (or just let them OOM?) Maybe on
// timeout we crash?
//
// We can ignore canceled here, I believe, and just work through anything left,
// but we should document this as a valid attitude to work in Turnstile.
//
// Writing things out again. Didn't occur to me
Journalist.prototype._locked = cadence(function (async, envelope) {
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
                    var directory = path.resolve(this.directory, 'pages', String(envelope.body))
                    async(function () {
                        mkdirp(directory, async())
                    }, function () {
                        var stream = fs.createWriteStream(path.resolve(directory, 'append'), { flags: 'a' })
                        var writable = new Staccato.Writable(stream)
                        async(function () {
                            async.forEach([ entry.writes ], function (write) {
                                writable.write(recorder({
                                    position: write.position,
                                    previous: write.previous,
                                    method: write.method,
                                    index: write.index,
                                    length: 0
                                }, write.serialized), async())
                            })
                        }, function () {
                            writable.end(async())
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

Journalist.prototype.append = function (entry) {
    var queue = this._queues[entry.id]
    if (queue == null) {
        var queue = this._queues[entry.id] = [{ method: 'write', writes: [], completed: new Signal }]
    }
    queue[0].writes.push(entry)
    this._lock.add(entry.id)
    return queue.signal
}

module.exports = Journalist
