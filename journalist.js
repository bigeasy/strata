var assert = require('assert')
var path = require('path')
var fs = require('fs')

var mkdirp = require('mkdirp')

var Staccato = require('staccato')

var Signal = require('signal')

var Turnstile = require('turnstile')
Turnstile.Set = require('turnstile/set')
Turnstile.Queue = require('turnstile/queue')

var cadence = require('cadence')
var sequester = require('sequester')

var Cache = require('magazine')

var Appender = require('./appender')
var Splitter = require('./splitter')

var Interrupt = require('interrupt').createInterrupter('b-tree')

var restrictor = require('restrictor')

var find = require('./find')

function Journalist (options) {
    this.magazine = new Cache().createMagazine()
    this.nextAddress = 0
    this.directory = options.directory
    this.cache = options.cache || new Cache()
    this.options = options
    this.comparator = options.comparator
    this._checksum = function () { return "0" }
    this.lengths = {}
    this.turnstiles = {
        lock: new Turnstile,
        housekeeping: new Turnstile
    }
    this.turnstile = new Turnstile
    this._locks = [  new Turnstile.Set(this, '_locked', this.turnstiles.lock) ]
    this._housekeeping = new Turnstile.Queue(this, '_tidy', this.turnstiles.housekeeping)
    this._queues = {}
    this._operationId = 0xffffffff
    this._blocks = []
}

function increment (value) {
    if (value == 0xffffffff) {
        return 0
    } else {
        return value + 1
    }
}

Journalist.prototype.read = cadence(function (async, id) {
    var filename = path.resolve(this.directory, 'pages', String(id), 'append')
    var items = [], heft = 0, leaf = +id.split('.')[1] % 2 == 1
    var readable = new Staccato.Readable(fs.createReadStream(filename))
    var splitter = new Splitter(function () { return '0' })
    async(function () {
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
        return { id: id, leaf: leaf, items: items, ghosts: 0, heft: heft }
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
    var descent = { miss: null, cartridges: [], index: 0, level: -1, keyed: null }, cartridge
    descent.cartridges.push(cartridge = this.magazine.hold(-1, null))
    for (;;) {
        if (descent.index != 0) {
            console.log('>', descent.keyed)
            descent.keyed = { key: page.items[descent.index].key, level: descent.level - 1 })
        }
        descent.level++
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

Journalist.prototype._writeLeaf = cadence(function (async, id, writes) {
    var page = queue.cartridge.value
    var directory = path.resolve(this.directory, 'pages', String(id))
    async(function () {
        mkdirp(directory, async())
    }, function () {
        var appender = new Appender(path.resolve(directory, 'append'))
        async(function () {
            async.forEach([ writes ], function (write) {
                appender.append(write.header, write.body, async())
            })
        }, function () {
            appender.end(async())
        })
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
    var entry = envelope.body
    async(function () {
        process.nextTick(async())
    }, function () {
        switch (envelope.method) {
        case 'pause':
            var block = this._blocks[envelope.index]
            delete this._blocks[envelope.index]
            block.enter.unlatch()
            block.pause.wait(async())
            break
        case 'write':
            async(function () {
                var queue = this._queues[entry.id]
                delete this._queues[entry.id]
                var cartridge = queue.cartridge, page = cartridge.value
                if (
                    page.items.length >= this.options.leaf.split ||
                    (
                        (page.id != '0.0' || page.right != null) &&
                        page.items.length <= this.options.leaf.merge
                    )
                ) {
                    this._housekeeping.add(entry.id)
                }
                this._writeLeaf(entry.body, entry.writes, async())
            }, function () {
                queue.exit.unlatch()
            })
            break
        }
    })
})

Journalist.prototype._queue = function (id) {
    var queue = this._queues[id]
    if (queue == null) {
        queue = this._queues[id] = {
            id: this._operationId = increment(this._operationId),
            method: 'write',
            writes: [],
            cartridge: this.magazine.hold(id, null),
            exit: new Signal
        }
        this._lock.add(id)
    }
    return queue
}

Journalist.prototype.lock = cadence(function (async, id) {
    var hash = id.split('.').reduce(function (sum, value) { return sum + +value }, 0)
    var index = hash % this._lcoks.length
    var block = this._blocks[index]
    if (block == null) {
        this._blocks[index] = { enter: new Signal, pause: new Signal }
    }
    return block
})

Journalist.prototype._tidy = cadence(function (async, id) {
    if (page.items.length >= this.options.leaf.split) {
        this._splitLeaf(id, async())
    } else if (page.items.length <= this.options.leaf.merge) {
        this._mergeLeaf(id, async())
    } // otherwise vaccum
})

function release (cartridge) { cartridge.release() }

Journalist.prototype._getPageAndParent = cadence(function (async, id, cartridges) {
    var page = { child: null, parent: null, keyed: null }
    async(function () {
        this.descend(key, -1, 0, async())
    }, function (descent) {
        cartridges.push(descent.cartridge)
        page.child = descent.cartridge.value
        page.keyed = descent.keyed
        this.descent(key, descent.level - 1, 0, async())
    }, function (descent) {
        cartridges.push(descent.cartridge)
        page.parent = descent.cartridge.value
        return page
    })
})

Journalist.prototype._mergeLeaf = cadence(function (async, id) {
    // Go to the leaf.
    var pages = [ null, null, null ], cartridges = [], branches = []
    async([function () {
        cartridges.forEach(release)
    }], function () {
        async(function () {
            this._getPageAndParent(id, cartridges, async())
        }, function (page) {
            pages[1] = page
            var siblings = []
            siblings.push(1)
            if (id != '0.0') {
                siblings.push(-1)
            }
            if (page.child.right != null) {
                siblings.push(1)
            }
            async.forEach([ siblings ], function (fork) {
                async(function () {
                    this._getPageAndParent(id, cartridges, async())
                }, function (page) {
                    page[1 + fork] = page
                })
            })
        })
        var pauses = pages.filter(function () { return page != null }).map(function (page) {
            this._pause(page.cartridge.value.id)
        })
        async([function () {
            pauses.forEach(function (pause) {
                pause.block.unlock()
            })
        }], function () {
            async.forEach([ pauses ], function (pause) { pause.enter.wait(async()) })
        }, function () {
            if (pages[1].child.items.length > this._options.leaf.split) {
                return [ async.break ]
            }
            // Choose the left or right for a merge.
            if (pages[2] == null) {
                from = pages[1]
                to = pages[0]
            } else if (
                pages[0] == null ||
                pages[0].child.items.length < pages[2].child.items.length
            ) {
                from = pages[2]
                to = pages[1]
            } else {
                from = pages[1]
                to = pages[0]
            }
            // Merge.
            to.right = from.right
            to.items.push.apply(to.items, from.items.slice(from.ghosts))
            // Remove the merged leaf from the parent.
            var descent = from.parent, shifted
            // Delete any empty branches along the way to the leaf page.
            while (descent.page.items.length == 1) {
                commit.push({ method: 'unlink', path: descent.page.id })
                descent = this._descend(descent.keyed.key, descent.keyed.level - 1, 0)
                cartridges.push.apply(cartridges, descent.cartridges)
            }
            // If branch item is the first entry, then the key of the second
            // entry should be promoted to the branch page above where the
            // locate key was found, otherwise we can just delete the entry.
            if (descent.index == 0) {
                var shifted = descent.page.items.shift()
                descent.page.items[descent.index].key = null
                commit.push({
                    method: 'splice',
                    id: descent.page.id,
                    vargs: [ 0, 1, descent.page.items[0] ]
                })
                descent = this._descend(descent.keyed.key, descent.keyed.level, 0)
                cartridges.push.apply(cartridges, descent.cartridges)
                descent.page.items[descent.index].key = shifted.key
                commit.push({
                    method: 'splice',
                    id: descent.page.id,
                    vargs: [ 0, 1, descent.page.items[descent.index] ]
                })
            } else {
                descent.page.items.splice(descent.index, 1)
                commit.push({
                    method: 'splice',
                    id: descent.page.id,
                    vargs: [ descent.index, 1 ]
                })
            }
            var commit = {}
            async(function () {
                // Append any writes to the two leaves we've merged, write them
                // to their existing files.
            }, function () {
                commit.write(this._instance, script, async())
            })
        })
    }, function () {
        async(function () {
            commit.prepare(async())
        }, function () {
            commit.commit(async())
        }, function () {
            if (descent.depth != 0 && descent.page.items.length < this._options.branch.merge) {
                this._mergeLeaf(descent.depth)
            }
        })
    })
})

Journalist.prototype.splitLeaf = cadence(function (async, envelope) {
    var page = cartridges[0].value, pages = 1, size
    while ((size = Math.floor(page.items.length / pages)) > this.options.leaf.split - 1) {
        pages++
    }
    if (pages != 1) {
        var begin = 0, splits = [], remainder = page.items.length % size, extra
        console.log(pages, size, remainder)
        for (var i = 0; i < pages; i++) {
            extra = remainder-- > 0 ? 1 : 0
            splits.push(page.items.slice(begin, size + extra))
            begin += size + extra
        }
        // TODO Not as expected, splits are inconsistent.
        console.log(splits)
        // TODO Adjust heft.
        // page.heft = // reduce heft
        page.items = splits[0]
        // Create new page cartridges.

        // Mark as no longer splitting.
        page.splitting = false

        // Load the parent leaf page.
        // Insert the new leaf key and write it out, here, just here.
        // Determine if you need to split the leaf.
    }
    break
})

Journalist.prototype.split = function (id) {
    var pause = this._pause(id)
    this._housekeeping.push({
        method: 'split',
        cartridge: this.magazine.hold(id, null),
        pause: this._pause(id)
    })
}

Journalist.prototype.append = function (entry, signals) {
    var queue = this._queue(entry.id)
    queue.writes.push(entry)
    if (signals[queue.id] == null) {
        signals[queue.id] = queue.exit
        queue.exit.notify(function () { delete signals[queue.id] })
    }
    this._lock.add(entry.id)
}

module.exports = Journalist
