var assert = require('assert')
var path = require('path')
var fs = require('fs')

var ascension = require('ascension')

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

var Commit = require('./commit')

function Journalist (strata, options) {
    this.strata = strata
    this.magazine = new Cache().createMagazine()
    this.nextAddress = 0
    this._id = 0
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
    this._locks = [  new Turnstile.Queue(this, '_locked', this.turnstiles.lock) ]
    this._blocks = [ {} ]
    this._housekeeping = new Turnstile.Set(this, '_tidy', this.turnstiles.housekeeping)
    this._queues = {}
    this._operationId = 0xffffffff
    this._blockId = 0xffffffff
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
    var cartridge, descent = {
        page: null,
        miss: null,
        cartridges: [],
        index: 0,
        level: -1,
        keyed: null
    }
    descent.cartridges.push(cartridge = this.magazine.hold(-1, null))
    for (;;) {
        if (descent.index != 0) {
            console.log('>', descent.keyed)
            descent.keyed = { key: page.items[descent.index].key, level: descent.level - 1 }
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
                    descent.cartridges.forEach(function (cartridge) { cartridge.release() })
                    return null
                }
            } else {
                if (++descent.index == page.items.length) {
                    descent.cartridges.forEach(function (cartridge) { cartridge.release() })
                    return null
                }
            }
        }
    }
    descent.page = descent.cartridges[descent.cartridges.length - 1].value
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

Journalist.prototype._writeLeaf = cadence(function (async, id, writes) {
    var directory = path.resolve(this.directory, 'pages', String(id))
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
        switch (entry.method) {
        case 'block':
            var block = this._blocks[entry.index][entry.id]
            delete this._blocks[entry.index][entry.id]
            block.enter.unlatch()
            block.exit.wait(async())
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
                    this._housekeeping.add(page.items[0].key)
                }
                this._writeLeaf(entry.body, entry.writes, async())
            }, function () {
                queue.exit.unlatch()
            })
            break
        }
    })
})

Journalist.prototype._index = function (id) {
    console.log(id)
    var hash = id.split('.').reduce(function (sum, value) { return sum + +value }, 0)
    return hash % this._locks.length
}

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
        this._locks[this._index(id)].push({ method: 'write', id: id })
    }
    return queue
}

Journalist.prototype._block = function (blockId, id) {
    var index = this._index(id)
    var block = this._blocks[index][blockId]
    if (block == null) {
        this._blocks[index][blockId] = block = { enter: new Signal, exit: new Signal }
        this._locks[index].push({ method: 'block', id: blockId, index: index })
    }
    return block
}

Journalist.prototype.append = function (entry, signals) {
    var queue = this._queue(entry.id)
    queue.writes.push(entry)
    if (signals[queue.id] == null) {
        signals[queue.id] = queue.exit
        queue.exit.notify(function () { delete signals[queue.id] })
    }
    var hash = entry.id.split('.').reduce(function (sum, value) { return sum + +value }, 0)
    var index = hash % this._locks.length
}

Journalist.prototype._getPageAndParent = cadence(function (async, key, level, fork, cartridges) {
    var lineage = { child: null, parent: null }
    async(function () {
        this.descend(key, level, fork, async())
    }, function (descent) {
        cartridges.push.apply(cartridges, descent.cartridges)
        lineage.child = descent
        this.descend(key, descent.level - 1, 0, async())
    }, function (descent) {
        cartridges.push.apply(cartridges, descent.cartridges)
        lineage.parent = descent
        return lineage
    })
})

Journalist.prototype._nextId = function (leaf) {
    do {
        var id = this._id++
    } while (id % 2 == leaf ? 1 : 0)
    return String(this._strata.instance) + '.' +  String(id)
}

Journalist.prototype._filename = function () {
}

Journalist.prototype._splitLeaf = cadence(function (async, lineage, cartridges) {
    async(function () {
        var commit = new Commit(this)
        async([function () {
            cartridges.forEach(function (cartridge) { cartridge.release() })
        }], function () {
            var blockId = this._blockId = increment(this._blockId)
            var block = this._block(blockId, lineage.child.page.id)
            async([function () {
                block.exit.unlock()
            }], function () {
                block.enter.wait(async())
            }, function () {
                var pages = [ lineage.child.page ]
                var partition = Math.floor(lineage.child.page.items.length / 2)
                var items = lineage.child.page.items.splice(partition)
                var heft = items.reduce(function (sum, item) { return sum + item.heft }, 0)
                var right = {
                    id: this._nextId(true),
                    leaf: true,
                    items: items,
                    right: lineage.child.page.right,
                    heft: heft,
                    append: this._filename()
                }
                page.push(right)
                left.right = right.items[0].key
                right.items[0].key = null
                cartridges.push(this.magazine.hold(right.id, right))
                lineage.parent.page.items.splice(lineage.parent.index + 1, 0, {
                    key: left.right,
                    id: left.id
                })
                pages.forEach(function (page) {
                    if (page.items.length >= this.options.leaf.split) {
                        this._housekeeping.add(page.items[0].key)
                    }
                }, this)
                var writes = this._queue(lineage.child.page.id).writes.splice(0)
                async(function () {
                    this._writeLeaf(lineage.child.page.id, writes, async())
                }, function () {
                    commit.write([[
                        'split', lineage.child.page.id, partition, right.id
                    ]], async())
                })
            })
        }, function () {
            async(function () {
                commit.prepare(async())
            }, function () {
                commit.commit(async())
            })
        })
    }, function () {
        if (lineage.parent.page.items.length >= this.options.branch.split) {
            if (lineage.parent.page.id == '0.0') {
                this._drainRoot(async())
            } else {
                this._splitBranch(lineage.parent, async())
            }
        }
    })
})

Journalist.prototype._fillRoot = cadence(function (async) {
})

Journalist.prototype._chooseMerge = function (lineages) {
    if (lineages[2] == null) {
        from = lineages[1]
        to = lineages[0]
    } else if (
        lineages[0] == null ||
        lineages[0].child.page.items.length < lineages[2].child.page.items.length
    ) {
        from = lineages[2]
        to = lineages[1]
    } else {
        from = lineages[1]
        to = lineages[0]
    }
    return { from: from, to: to }
}

Journalist.prototype._removePivot = function (parent, script, cartridges) {
    // Delete any empty branches along the way to the leaf page.
    while (parent.page.items.length == 1) {
        script.push([ 'unlink', parent.page.id ])
        parent = this._descend(parent.keyed.key, parent.keyed.level - 1, 0)
        cartridges.push.apply(cartridges, parent.cartridges)
    }
    // If branch item is the first entry, then the key of the second
    // entry should be promoted to the branch page above where the
    // locate key was found, otherwise we can just delete the entry.
    if (parent.index == 0) {
        var shifted = parent.page.items.shift()
        parent.page.items[parent.index].key = null
        script.push([ 'splice', parent.page.id, [ 0, 1, parent.page.items[0] ] ])
        parent = this._descend(parent.keyed.key, parent.keyed.level, 0)
        cartridges.push.apply(cartridges, parent.cartridges)
        parent.page.items[parent.index].key = shifted.key
        script.push([ 'splice', parent.page.id, [ 0, 1, parent.page.items[parent.index] ] ])
    } else {
        parent.page.items.splice(parent.index, 1)
        script.push([ 'splice', parent.page.id, [ parent.index, 1 ] ])
    }
    return parent
}

Journalist.prototype._mergeBranch = cadence(function (async, keyed) {
    var cartridges = [], parent
    async([function () {
        cartridges.forEach(function (cartridge) { cartridge.release() })
    }], function () {
        var lineages = [ null, null, null ]
        async(function () {
            this._getPageAndParent(keyed.key, keyed.level, 0, async())
        }, function (lineage) {
            lineages[1] = lineage
            async.forEach([ 1, -1 ], function (fork) {
                async(function () {
                    this._getPageAndParent(keyed.key, keyed.level, fork, async())
                }, function (lineage) {
                    lineages[1 + fork] = lineage
                })
            })
        }, function () {
            // Choose the left or right for a merge.
            var merge = this._chooseMerge(lineages), from = merge.from, to = merge.to
            // Merge page.
            to.child.page.items.push.apply(to.child.page.items, from.child.page.items.slice(from.child.page.ghosts))
            // Record for file rewriting.
            script.push([ 'merge', to.child.page.id, from.child.page.id ])
            // Remove the pivot of the deleted branch.
            parent = this._removePivot(parent, script, cartridge)
            // Write commit.
            commit.write(script, async())
        }, function () {
            commit.prepare(async())
        }, function () {
            commit.commit(async())
        })
    }, function () {
        if (parent.page.id == '0.0') {
            if (parent.page.items.length == 1) {
                this._fillRoot(keyed.key, async())
            }
        } else if (parent.page.items.length <= this.options.branch.merge) {
            this._mergeBranch(parent.keyed, async())
        }
    })
})

Journalist.prototype._mergeLeaf = cadence(function (async, key, lineage, cartridges) {
    // Go to the leaf.
    var lineages = [ null, lineage, null ], cartridges = [], branches = []
    async(function () {
        var commit = new Commit(this)
        async([function () {
            cartridges.forEach(function (cartridge) { cartridge.release() })
        }], function () {
            async(function () {
                var siblings = []
                if (lineage.child.page.id != '0.0') {
                    siblings.push(-1)
                }
                if (lineage.child.page.right != null) {
                    siblings.push(1)
                }
                async.forEach([ siblings ], function (fork) {
                    async(function () {
                        this._getPageAndParent(key, -1, fork, cartridges, async())
                    }, function (lineage) {
                        lineages[1 + fork] = lineage
                    })
                })
            })
            var blockId = this._blockId = increment(this._blockId)
            var blocks = pages.filter(function (lineage) { return lineage != null }).map(function (lineage) {
                return this._block(blockId, lineage.child.page.id)
            })
            async([function () {
                blocks.forEach(function (block) { block.exit.unlock() })
            }], function () {
                async.forEach([ blocks ], function (block) { block.enter.wait(async()) })
            }, function () {
                if (lineages[1].child.items.length > this.options.leaf.merge) {
                    return [ async.break ]
                }
                // Choose the left or right for a merge.
                var merge = this._chooseMerge(lineages), from = merge.from, to = merge.to
                // Merge.
                to.child.page.right = from.child.page.right
                to.child.page.items.push.apply(to.child.page.items, from.child.page.items.slice(from.child.page.ghosts))
                // Remove the merged leaf from the parent.
                var parent = this._removePivot(parent, script, cartridges)
                if (to.child.page.items.length >= this.options.leaf.split) {
                    this._housekeeping.add(to.child.page.items[0].key)
                }
                var writes = ([ to, from ]).map(function (lineage) {
                    return {
                        id: lineage.child.page.id,
                        writes: this._queue(lineage.child.page.id).writes.splice(0)
                    }
                })
                var commit = {}
                async(function () {
                    // Append any writes to the two leaves we've merged, write
                    // them to their existing files.
                    async.forEach([ writes ], function (write) {
                        this._writeLeaf(write.id, write.writes, async())
                    })
                }, function () {
                    commit.write(script, async())
                })
            })
        }, function () {
            async(function () {
                commit.prepare(async())
            }, function () {
                commit.commit(async())
            }, function () {
            })
        })
    }, function () {
        if (lineage.parent.page.id != '0.0' && lineage.parent.page.items.length <= this.options.branch.merge) {
            this._mergeBranch(lineage.parent.keyed, async())
        }
    })
})

Journalist.prototype._tidy = cadence(function (async, key) {
    var cartridges = []
    async([function () {
        delete this._dirty[envelope.body.id]
    }], function () {
        this._getPageAndParent(key, -1, 0, cartridges, async())
    }, function (lineage) {
        if (
            lineage.child.page.items.length >= this.options.leaf.split
        ) {
            this._splitLeaf(lineage, cartridges, async())
        } else if (
            lineage.child.page.items.length <= this.options.leaf.merge
        ) {
            this._mergeLeaf(key, lineage, cartridges, async())
        } // if vacuum
    })
})

module.exports = Journalist
