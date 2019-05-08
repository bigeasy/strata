var assert = require('assert')
var path = require('path')
var fs = require('fs')

var ascension = require('ascension')

var mkdirp = require('mkdirp')

var Staccato = require('staccato')

var Signal = require('signal')

var Turnstile = require('turnstile')
Turnstile.Set = require('turnstile/set')

var cadence = require('cadence')
var sequester = require('sequester')

var Cache = require('magazine')

var Appender = require('./appender')
var Splitter = require('./splitter')

var Interrupt = require('interrupt').createInterrupter('b-tree')

var restrictor = require('restrictor')

var find = require('./find')

function Journalist (strata, options) {
    this.strata = strata
    this.magazine = new Cache().createMagazine()
    this.nextAddress = 0
    this.directory = options.directory
    this.cache = options.cache || new Cache()
    this.options = options
    this.comparator = options.comparator
    this._checksum = function () { return "0" }
    this.lengths = {}
    this.turnstiles = {
        lock: new Turnstile
    }
    this.turnstile = new Turnstile
    this._lock = new Turnstile.Set(this, '_locked', this.turnstiles.lock)
    this._queues = {}
    this._operationId = 0xffffffff
}

function increment (value) {
    if (value == 0xffffffff) {
        return 0
    } else {
        return value + 1
    }
}

Journalist.prototype.write = cadence(function (async, page) {
    var append = String(this.instance) + '.' + String(Date.now())
    var filename = path.resolve('pages', String(id), append)
    var appender = new Appender(path.resolve(this.directory, filename))
    async(function () {
        async.forEach([ page.items ], function (item, index) {
            appender.append({
                method: 'insert',
                index: index,
                value: { key: item.key, id: item.id }
            }, null, async())
        })
    }, function () {
        appender.end(async())
    }, function () {
        return { page: page, append: append }
    })
})

Journalist.prototype.read = cadence(function (async, id) {
    var directory = path.resolve(this.directory, 'pages', String(id))
    var items = [], heft = 0, leaf = +id.split('.')[1] % 2 == 1
    var splitter = new Splitter(function () { return '0' })
    async(function () {
        this._appendable(id, async())
    }, function (append) {
        async(function () {
            var filename = path.join(directory, append)
            var readable = new Staccato.Readable(fs.createReadStream(filename))
            async.loop([], function () {
                async(function () {
                    readable.read(async())
                }, function (chunk) {
                    if (chunk == null) {
                        readable.raise()
                        return [ async.break ]
                    }
                    splitter.split(chunk).forEach(function (entry) {
                        switch (entry.header.method) {
                        case 'insert':
                            if (leaf) {
                                items.splice(entry.header.index, 0, {
                                    key: entry.body.key,
                                    value: entry.body.value,
                                    heft: entry.sizes[1]
                                })
                                heft += entry.sizes[1]
                            } else {
                                items.splice(entry.header.index, 0, {
                                    id: entry.header.value.id,
                                    key: entry.header.value.key,
                                    heft: entry.sizes[0]
                                })
                                heft += entry.sizes[0]
                            }
                        }
                    })
                })
            })
        }, function () {
            // TODO Did we ghost? Not really checking.
            return { id: id, leaf: leaf, items: items, ghosts: 0, heft: heft, append: append }
        })
    })
})

Journalist.prototype.load = restrictor.enqueue('canceled', cadence(function (async, id) {
    var cartridge = this.magazine.hold(id, null)
    async([function () {
        cartridge.release()
    }], function () {
        if (cartridge.value != null) {
            return [ async.return ]
        }
        async(function () {
            this.read(id, async())
        }, function (page) {
            cartridge.value = page
            cartridge.adjustHeft(page.heft)
            return []
        })
    })
}))

Journalist.prototype._descend = function (key, level, fork) {
    var descent = { miss: null, cartridges: [], index: 0, level: 0, keyed: null }, cartridge
    descent.cartridges.push(cartridge = this.magazine.hold(-1, null))
    for (;;) {
        if (descent.index != 0) {
            console.log('>', descent.keyed)
            descent.keyed = page.items[descent.index].key
        }
        var id = cartridge.value.items[descent.index].id
        descent.cartridges.push(cartridge = this.magazine.hold(id, null))
        if (cartridge.value == null) {
            descent.cartridges.pop().remove()
            descent.miss = id
            return descent
        }
        var page = cartridge.value
        // TODO Maybe page offset instead of ghosts, nah leave it so you remember it.
        descent.index = find(this.options.comparator, page, key, page.leaf ? page.ghosts : 1)
        if (page.leaf) {
            assert.equal(level, -1, 'could not find branch')
            break
        } else if (level == descent.level) {
            break
        } else if (descent.index < 0) {
            // On a branch, unless we hit the key exactly, we're
            // pointing at the insertion point which is right after the
            // branching we're supposed to decend, so back it up one
            // unless it's a bullseye.
            descent.index = ~descent.index - 1
        } else if (fork != 0) {
            if (fork < 0) {
                if (descent.index-- == 0) {
                    return null
                }
            } else {
                if (++descent.index == page.items.length) {
                    return null
                }
            }
        }
        descent.level++
    }
    return descent
}

Journalist.prototype.descend = cadence(function (async, key, level, fork) {
    var cartridges = []
    async.loop([], function () {
        var descent = this._descend(key, level, fork)
        cartridges.forEach(function (cartridge) { cartridge.release() })
        if (descent.miss == null) {
            return [ async.break, descent ]
        }
        cartridges = descent.cartridges
        this.load(descent.miss, async())
    })
})

var appendable = ascension([ Number, Number ], function (file) {
    return file.split('.')
})

Journalist.prototype._appendable = cadence(function (async, id) {
    async(function () {
        fs.readdir(path.join(this.directory, 'pages', id), async())
    }, function (dir) {
        return dir.filter(function (file) {
            return /^\d+\.\d+$/.test(file)
        }).sort(appendable).pop()
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
    async(function () {
        async.loop([], function () {
            if (queue.length == 0) {
                return [ async.break ]
            }
            var entry = queue.shift()
            async(function () {
                switch (entry.method) {
                case 'write':
                    var directory = path.resolve(this.directory, 'pages', String(envelope.body))
                    async(function () {
                        this._appendable(envelope.body, async())
                    }, function (append) {
                        var appender = new Appender(path.resolve(directory, append))
                        async(function () {
                            async.forEach([ entry.writes ], function (write) {
                                appender.append(write.header, write.body, async())
                            })
                        }, function () {
                            appender.end(async())
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

Journalist.prototype.append = function (entry, signals) {
    var queue = this._queues[entry.id]
    if (queue == null) {
        queue = this._queues[entry.id] = []
    }
    if (queue.length == 0) {
        queue.push({
            id: this._operationId = increment(this._operationId),
            method: 'write',
            writes: [],
            completed: new Signal
        })
    }
    queue[0].writes.push(entry)
    if (signals[queue[0].id] == null) {
        signals[queue[0].id] = queue[0].completed
    }
    this._lock.add(entry.id)
}

module.exports = Journalist
