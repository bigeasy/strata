var Cache = require('magazine'),
    Journalist = require('journalist'),
    cadence = require('cadence'),
    ok = require('assert').ok,
    path = require('path'),
    prototype = require('pointcut').prototype

// todo: temporary
var scram = require('./scram')

function extend(to, from) {
    for (var key in from) to[key] = from[key]
    return to
}

var __slice = [].slice

/*function say() {
        var args = __slice.call(arguments)
        console.log(require('util').inspect(args, false, null))
}*/

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

function extract (a) { return a }

function Locker (sheaf, magazine) {
    ok(arguments.length, 'no arguments')
    ok(magazine)
    this._locks = {}
    this._sheaf = sheaf
    this._magazine = magazine
}

prototype(Locker, 'lock', cadence(function (async, address, exclusive) {
    var cartridge = this._magazine.hold(address, {}),
        page = cartridge.value.page,
        locked

    ok(address != null, 'x' + address)
    ok(!this._locks[address], 'address already locked by this locker')

    if (!page)  {
        if (address % 2) {
            page = this._sheaf.createLeaf({ address: address })
        } else {
            page = this._sheaf.createBranch({ address: address })
        }
        cartridge.value.page = page
        var loaded = function (error) {
            if (error) {
                cartridge.value.page = null
                cartridge.adjustHeft(-cartridge.heft)
            }
            this._locks[page.address].unlock(error, page)
        }.bind(this)
        this._locks[page.address] = page.queue.createLock()
        this._locks[page.address].exclude(function () {
            if (page.address % 2) {
                this._sheaf.readLeaf(page, loaded)
            } else {
                this._sheaf.readBranch(page, loaded)
            }
        }.bind(this))
    } else {
        this._locks[page.address] = page.queue.createLock()
    }

    async([function () {
        async(function () {
            this._locks[page.address][exclusive ? 'exclude' : 'share'](async())
        },
        function () {
            this._sheaf.tracer('lock', { address: address, exclusive: exclusive }, async())
        }, function () {
            locked = true
            return [ page ]
        })
    }, function (errors, error) {
        // todo: if you don't return something, then the return is the
        // error, but what else could it be? Document that behavior, or
        // set a reasonable default.
        this._magazine.get(page.address).release()
        this._locks[page.address].unlock(error)
        delete this._locks[page.address]
        throw errors
    }])
}))

Locker.prototype.encache = function (page) {
    this._magazine.hold(page.address, { page: page })
    this._locks[page.address] = page.queue.createLock()
    this._locks[page.address].exclude(function () {})
    return page
}

Locker.prototype.checkCacheSize = function (page) {
    var heft = page.items.reduce(function (heft, item) { return heft + item.heft }, 0)
    ok(heft == this._magazine.get(page.address).heft, 'sizes are wrong')
}

Locker.prototype.unlock = function (page) {
    this.checkCacheSize(page)
    this._locks[page.address].unlock(null, page)
    if (!this._locks[page.address].count) {
        delete this._locks[page.address]
    }
    this._magazine.get(page.address).release()
}

Locker.prototype.increment = function (page) {
    this._locks[page.address].increment()
    this._magazine.hold(page.address)
}

Locker.prototype.dispose = function () {
    ok(!Object.keys(this._locks).length, 'locks outstanding')
    this._locks = null
}

function Cursor (sheaf, journal, descents, exclusive, searchKey) {
    this._journal = journal
    this._sheaf = sheaf
    this._locker = descents[0].locker
    this._page = descents[0].page
    this._rightLeafKey = null
    this._searchKey = searchKey
    this.exclusive = exclusive
    this.index = descents[0].index
    this.offset = this.index < 0 ? ~ this.index : this.index

    descents.shift()
}

Cursor.prototype.get = function (index) {
    return this._page.items[index]
}

// to user land
prototype(Cursor, 'next', cadence(function (async) {
    var next
    this._rightLeafKey = null

    if (!this._page.right) {
        // return [ async, false ] <- return immediately!
        return [ false ]
    }

    async(function () {
        this._locker.lock(this._page.right, this.exclusive, async())
    }, function (next) {
        this._locker.unlock(this._page)

        this._page = next

        this.offset = this._page.ghosts
        this.length = this._page.items.length

        return [ true ]
    })
}))

// to user land
prototype(Cursor, 'indexOf', cadence(function (async, key) {
    async(function () {
        return this._sheaf.find(this._page, key, this._page.ghosts)
    }, function (index) {
        var unambiguous
        unambiguous = -1 < index
                   || ~ index < this._page.items.length
                   || ! this._page.right
                   || this._searchKey.length && this._sheaf.comparator(this._searchKey[0], key) == 0
        if (!unambiguous) async(function () {
            if (!this._rightLeafKey) async(function () {
                this._locker.lock(this._page.right, false, async())
            }, function (rightLeafPage) {
                async([function () {
                    this._locker.unlock(rightLeafPage)
                }], function (entry) {
                    this._rightLeafKey = rightLeafPage.items[0].key
                })
            })
        }, function  () {
            if (this._sheaf.comparator(key, this._rightLeafKey) >= 0) {
                return [ ~(this._page.items.length + 1) ]
            } else {
                return index
            }
        })
    })
}))

// todo: pass an integer as the first argument to force the arity of the
// return.
prototype(Cursor, '_unlock', cadence(function (async) {
    async([function () {
        this._locker.unlock(this._page)
        this._locker.dispose()
    }], function () {
        this._journal.close('leaf', async(0))
    })
}))

prototype(Cursor, 'unlock',  function (callback) {
    ok(callback, 'unlock now requires a callback')
    this._unlock(callback)
})

// note: exclusive, index, offset and length are public

Cursor.prototype.__defineGetter__('address', function () {
    return this._page.address
})

Cursor.prototype.__defineGetter__('right', function () {
    return this._page.right
})

Cursor.prototype.__defineGetter__('ghosts', function () {
    return this._page.ghosts
})

Cursor.prototype.__defineGetter__('length', function () {
    return this._page.items.length
})

prototype(Cursor, 'insert', cadence(function (async, record, key, index) {
    ok(this.exclusive, 'cursor is not exclusive')
    ok(index > 0 || this._page.address == 1)

    var entry
    this._sheaf.unbalanced(this._page)
    async(function () {
        entry = this._journal.open(this._sheaf._filename(this._page.address, 0), this._page.position, this._page)
        this._sheaf.journalist.purge(async())
    }, function () {
        entry.ready(async())
    }, function () {
        scram.call(this, entry, cadence(function (async) {
            async(function () {
                this._sheaf.writeInsert(entry, this._page, index, record, async())
            }, function (position, length, size) {
                this._sheaf.splice(this._page, index, 0, {
                    key: key,
                    record: record,
                    heft: length
                })
                this.length = this._page.items.length
            }, function () {
                async(function () {
                    entry.close('entry', async())
                }, function () {
                    return []
                })
            })
        }), async())
    })
}))

prototype(Cursor, 'remove', cadence(function (async, index) {
    var ghost = this._page.address != 1 && index == 0, entry
    this._sheaf.unbalanced(this._page)
    async(function () {
        this._sheaf.journalist.purge(async())
    }, function () {
        entry = this._journal.open(this._sheaf._filename(this._page.address, 0), this._page.position, this._page)
        entry.ready(async())
    }, function () {
        scram.call(this, entry, cadence(function (async) {
            async(function () {
                this._sheaf.writeDelete(entry, this._page, index, async())
            }, function () {
                if (ghost) {
                    this._page.ghosts++
                    this.offset || this.offset++
                } else {
                    this._sheaf.splice(this._page, index, 1)
                }
            }, function () {
                entry.close('entry', async())
            })
        }), async())
    }, function () {
        // todo: arity in delclaration.
        return []
    })
}))

function Descent (sheaf, locker, override) {
    ok(locker instanceof Locker)

    override = override || {}

    this. exclusive = override.exclusive || false
    this.depth = override.depth == null ? -1 : override.depth
    this.indexes = override.indexes || {}
    this.sheaf = sheaf
    this.greater = override.greater
    this.lesser = override.lesser
    this.page = override.page
    this._index = override.index == null ? 0 : override.index
    this.locker = locker
    this.descent = {}

    if (!this.page) {
        this.locker.lock(-2, false, function (error, page) {
            ok(!error, 'impossible error')
            this.page = page
        }.bind(this))
        ok(this.page, 'dummy page not in cache')
    } else {
        this.locker.increment(this.page)
    }
}

Descent.prototype.__defineSetter__('index', function (i) {
    this.indexes[this.page.address] = this._index = i
})

Descent.prototype.__defineGetter__('index', function () {
    return this._index
})

Descent.prototype.fork = function () {
    return new Descent(this.sheaf, this.locker, {
        page: this.page,
        exclusive: this.exclusive,
        depth: this.depth,
        index: this.index,
        indexes: extend({}, this.indexes)
    })
}

Descent.prototype.exclude = function () {
    this.exclusive = true
}

prototype(Descent, 'upgrade', cadence(function (async) {
    async([function () {
        this.locker.unlock(this.page)
        this.locker.lock(this.page.address, this.exclusive = true, async())
    }, function (errors) {
        this.locker.lock(-2, false, function (error, locked) {
            ok(!error, 'impossible error')
            this.page = locked
        }.bind(this))
        ok(this.page, 'dummy page not in cache')
        throw errors
    }], function (locked) {
        this.page = locked
    })
}))

Descent.prototype.key = function (key) {
    return function (callback) {
        callback(null, this.sheaf.find(this.page, key, this.page.address % 2 ? this.page.ghosts : 1))
    }
}

Descent.prototype.left = function (callback) { callback(null, this.page.ghosts || 0) }

Descent.prototype.right = function (callback) { callback(null, this.page.items.length - 1) }

Descent.prototype.found = function (keys) {
    return function () {
        return this.page.items[0].address != 0 && this.index != 0 && keys.some(function (key) {
            return this.sheaf.comparator(this.page.items[this.index].key,  key) == 0
        }, this)
    }
}

Descent.prototype.child = function (address) { return function () { return this.page.items[this.index].address == address } }

Descent.prototype.address = function (address) { return function () { return this.page.address == address } }

Descent.prototype.penultimate = function () { return this.page.items[0].address % 2 }

Descent.prototype.leaf = function () { return this.page.address % 2 }

Descent.prototype.level = function (level) {
    return function () { return level == this.depth }
}

Descent.prototype.unlocker = function (parent) {
    this.locker.unlock(parent)
}

prototype(Descent, 'descend', cadence(function (async, next, stop) {
    var above = this.page

    var loop = async(function () {
        if (stop.call(this)) {
            return [ loop, this.page, this.index ]
        } else {
            if (this.index + 1 < this.page.items.length) {
                this.greater = this.page.address
            }
            if (this.index > 0) {
                this.lesser = this.page.address
            }
            this.locker.lock(this.page.items[this.index].address, this.exclusive, async())
        }
    }, function (locked) {
        this.depth++
        this.unlocker(this.page, locked)
        this.page = locked
        next.call(this, async())
    }, function (index) {
        if (!(this.page.address % 2) && index < 0) {
            this.index = (~index) - 1
        } else {
            this.index = index
        }
        this.indexes[this.page.address] = this.index
        if (!(this.page.address % 2)) {
            ok(this.page.items.length, 'page has addresses')
            ok(this.page.items[0].key == null, 'first key is cached')
        }
    })()
}))

prototype(Sheaf, 'unbalanced', function (page, force) {
    if (force) {
        this.lengths[page.address] = this.options.leafSize
    } else if (this.lengths[page.address] == null) {
        this.lengths[page.address] = page.items.length - page.ghosts
    }
})

prototype(Sheaf, '_nodify', cadence(function (async, locker, page) {
    async(function () {
        async([function () {
            locker.unlock(page)
        }], function () {
            var entry
            ok(page.address % 2, 'leaf page expected')

            if (page.address == 1) entry = {}
            else entry = page.items[0]

            var node = {
                key: entry.key,
                address: page.address,
                rightAddress: page.right,
                length: page.items.length - page.ghosts
            }
            this.ordered[node.address] = node
            if (page.ghosts) {
                this.ghosts[node.address] = node
            }
            return [ node ]
        })
    }, function (node) {
        async(function () {
            this.tracer('reference', {}, async())
        }, function () {
            return node
        })
    })
}))

// to user land
prototype(Sheaf, 'balance', cadence(function balance (async) {
    var locker = this.createLocker(), operations = [], address, length

    var _gather = cadence(function (async, address, length) {
        var right, node
        async(function () {
            if (node = this.ordered[address]) {
                return [ node ]
            } else {
                async(function () {
                    locker.lock(address, false, async())
                }, function (page) {
                    this._nodify(locker, page, async())
                })
            }
        }, function (node) {
            if (!(node.length - length < 0)) return
            if (node.address != 1 && ! node.left) async(function () {
                var descent = new Descent(this, locker)
                async(function () {
                    descent.descend(descent.key(node.key), descent.found([node.key]), async())
                }, function () {
                    descent.index--
                    descent.descend(descent.right, descent.leaf, async())
                }, function () {
                    var left
                    if (left = this.ordered[descent.page.address]) {
                        locker.unlock(descent.page)
                        return [ left ]
                    } else {
                        this._nodify(locker, descent.page, async())
                    }
                }, function (left) {
                    left.right = node
                    node.left = left
                })
            })
            if (!node.right && node.rightAddress) async(function () {
                if (right = this.ordered[node.rightAddress]) return [ right ]
                else async(function () {
                    locker.lock(node.rightAddress, false, async())
                }, function (page) {
                    this._nodify(locker, page, async())
                })
            }, function (right) {
                node.right = right
                right.left = node
            })
        })
    })

    ok(!this.balancing, 'already balancing')

    var lengths = this.lengths, addresses = Object.keys(lengths)
    if (addresses.length == 0) {
        return [ async, true ]
    } else {
        this.lengths = {}
        this.operations = []
        this.ordered = {}
        this.ghosts = {}
        this.balancing = true
    }

    async(function () {
        async(function (address) {
            _gather.call(this, +address, lengths[address], async())
        })(addresses)
    }, function () {
        this.tracer('plan', {}, async())
    }, function () {
        var address, node, difference, addresses

        for (address in this.ordered) {
            node = this.ordered[address]
        }

        function terminate (node) {
            var right
            if (node) {
                if (right = node.right) {
                    node.right = null
                    right.left = null
                }
            }
            return right
        }

        function unlink (node) {
            terminate(node.left)
            terminate(node)
            return node
        }

        for (address in lengths) {
            length = lengths[address]
            node = this.ordered[address]
            difference = node.length - length
            if (difference > 0 && node.length > this.options.leafSize) {
                operations.unshift({
                    method: 'splitLeaf',
                    parameters: [ node.address, node.key, this.ghosts[node.address] ]
                })
                delete this.ghosts[node.address]
                unlink(node)
            }
        }

        for (address in this.ordered) {
            if (this.ordered[address].left) delete this.ordered[address]
        }

        for (address in this.ordered) {
            var node = this.ordered[address]
            while (node && node.right) {
                if (node.length + node.right.length > this.options.leafSize) {
                    node = terminate(node)
                    this.ordered[node.address] = node
                } else {
                    if (node = terminate(node.right)) {
                        this.ordered[node.address] = node
                    }
                }
            }
        }

        for (address in this.ordered) {
            node = this.ordered[address]

            if (node.right) {
                ok(!node.right.right, 'merge pair still linked to sibling')
                operations.unshift({
                    method: 'mergeLeaves',
                    parameters: [ node.right.key, node.key, lengths, !!this.ghosts[node.address] ]
                })
                delete this.ghosts[node.address]
                delete this.ghosts[node.right.address]
            }
        }

        for (address in this.ghosts) {
            node = this.ghosts[address]
            if (node.length) operations.unshift({
                method: 'deleteGhost',
                parameters: [ node.key ]
            })
        }

        async(function () {
            async(function (operation) {
                this[operation.method].apply(this, operation.parameters.concat(async()))
            })(operations)
        }, function () {
            this.balancing = false
            return false
        })
    })
}))

prototype(Sheaf, 'shouldSplitBranch', function (branch, key, callback) {
    if (branch.items.length > this.options.branchSize) {
        if (branch.address == 0) {
            this.drainRoot(callback)
        } else {
            this.splitBranch(branch.address, key, callback)
        }
    } else {
        callback(null)
    }
})

prototype(Sheaf, 'splitLeaf', cadence(function (async, address, key, ghosts) {
    var locker = this.createLocker(),
        descents = [], replacements = [], encached = [],
        completed = 0,
        penultimate, leaf, split, pages, page,
        records, remainder, right, index, offset, length

    async(function () {
        async([function () {
            encached.forEach(function (page) { locker.unlock(page) })
            descents.forEach(function (descent) { locker.unlock(descent.page) })
            locker.dispose()
        }], function () {
            if (address != 1 && ghosts) async(function () {
                this.deleteGhost(key, async())
            }, function (rekey) {
                key = rekey
            })
        }, function () {
            descents.push(penultimate = new Descent(this, locker))

            penultimate.descend(address == 1 ? penultimate.left : penultimate.key(key),
                                penultimate.penultimate, async())
        }, function () {
            penultimate.upgrade(async())
        }, function () {
            descents.push(leaf = penultimate.fork())
            leaf.descend(address == 1 ? leaf.left : leaf.key(key), leaf.leaf, async())
        }, function () {
            split = leaf.page
            if (split.items.length - split.ghosts <= this.options.leafSize) {
                this.unbalanced(split, true)
                return [ async ]
            }
        }, function () {
            pages = Math.ceil(split.items.length / this.options.leafSize)
            records = Math.floor(split.items.length / pages)
            remainder = split.items.length % pages

            right = split.right

            offset = split.items.length

            async(function () {
                page = locker.encache(this.createLeaf({ loaded: true }))
                encached.push(page)

                page.right = right
                right = page.address

                length = remainder-- > 0 ? records + 1 : records
                offset = split.items.length - length
                index = offset

                this.splice(penultimate.page, penultimate.index + 1, 0, {
                    key: split.items[offset].key,
                    heft: this.serialize(split.items[offset].key, true).length,
                    address: page.address
                })

                async(function () {
                    var item = split.items[index]

                    ok(index < split.items.length)

                    this.splice(page, page.items.length, 0, item)

                    index++
                })(length)
            }, function () {
                this.splice(split, offset, length)

                replacements.push(page)

                this.rewriteLeaf(page, '.replace', async())
            })(pages - 1)
        }, function () {
            split.right = right

            replacements.push(split)

            this.rewriteLeaf(split, '.replace', async())
        }, function () {
            this.writeBranch(penultimate.page, '.pending', async())
        }, function () {
            this.tracer('splitLeafCommit', {}, async())
        }, function () {
            this._rename(penultimate.page, 0, '.pending', '.commit', async())
        }, function () {
            async(function (page) {
                this.replace(page, '.replace', async())
            })(replacements)
        }, function () {
            this.replace(penultimate.page, '.commit', async())
        }, function () {
            this.unbalanced(leaf.page, true)
            this.unbalanced(page, true)
            return [ encached[0].items[0].key ]
        })
    }, function (partition) {
        this.shouldSplitBranch(penultimate.page, partition, async())
    })
}))

prototype(Sheaf, 'splitBranch', cadence(function (async, address, key) {
    var locker = this.createLocker(),
        descents = [],
        children = [],
        encached = [],
        parent, full, split, pages,
        records, remainder, offset,
        unwritten, pending

    async(function () {
        async([function () {
            encached.forEach(function (page) { locker.unlock(page) })
            descents.forEach(function (descent) { locker.unlock(descent.page) })
            locker.dispose()
        }], function () {
            descents.push(parent = new Descent(this, locker))
            parent.descend(parent.key(key), parent.child(address), async())
        }, function () {
            parent.upgrade(async())
        }, function () {
            descents.push(full = parent.fork())
            full.descend(full.key(key), full.level(full.depth + 1), async())
        }, function () {
            split = full.page

            pages = Math.ceil(split.items.length / this.options.branchSize)
            records = Math.floor(split.items.length / pages)
            remainder = split.items.length % pages

            offset = split.items.length

            for (var i = 0; i < pages - 1; i++ ) {
                var page = locker.encache(this.createBranch({}))

                children.push(page)
                encached.push(page)

                var length = remainder-- > 0 ? records + 1 : records
                var offset = split.items.length - length

                var cut = this.splice(split, offset, length)

                this.splice(parent.page, parent.index + 1, 0, {
                    key: cut[0].key,
                    address: page.address,
                    heft: cut[0].heft
                })

                delete cut[0].key
                cut[0].heft = 0

                this.splice(page, 0, 0, cut)
            }
        }, function () {
            children.unshift(full.page)
            async(function (page) {
                this.writeBranch(page, '.replace', async())
            })(children)
        }, function () {
            this.writeBranch(parent.page, '.pending', async())
        }, function () {
            this._rename(parent.page, 0, '.pending', '.commit', async())
        }, function () {
            async(function (page) {
                this.replace(page, '.replace', async())
            })(children)
        }, function () {
            this.replace(parent.page, '.commit', async())
        })
    }, function () {
        this.shouldSplitBranch(parent.page, key, async())
    })
}))

prototype(Sheaf, 'drainRoot', cadence(function (async) {
    var locker = this.createLocker(),
        children = [], locks = [],
        root, pages, records, remainder

    async(function () {
        async([function () {
            children.forEach(function (page) { locker.unlock(page) })
            locks.forEach(function (page) { locker.unlock(root) })
            locker.dispose()
        }], function () {
            locker.lock(0, true, async())
        }, function (locked) {
            locks.push(root = locked)
            pages = Math.ceil(root.items.length / this.options.branchSize)
            records = Math.floor(root.items.length / pages)
            remainder = root.items.length % pages
            var lift = []

            for (var i = 0; i < pages; i++) {
                var page = locker.encache(this.createBranch({}))

                var length = remainder-- > 0 ? records + 1 : records
                var offset = root.items.length - length

                var cut = this.splice(root, offset, length)

                lift.push({
                    key: cut[0].key,
                    address: page.address,
                    heft: cut[0].heft
                })
                children.push(page)

                delete cut[0].key
                cut[0].heft = 0

                this.splice(page, 0, 0, cut)
            }

            lift.reverse()

            this.splice(root, 0, 0, lift)
        }, function () {
            async(function (page) {
                this.writeBranch(page, '.replace', async())
            })(children)
        }, function () {
            this.writeBranch(root, '.pending', async())
        }, function () {
            this._rename(root, 0, '.pending', '.commit', async())
        }, function () {
            async(function (page) {
                this.replace(page, '.replace', async())
            })(children)
        }, function () {
            this.replace(root, '.commit', async())
        })
    }, function () {
        if (root.items.length > this.options.branchSize) this.drainRoot(async())
    })
}))

prototype(Sheaf, 'exorcise', cadence(function (async, pivot, page, corporal) {
    var entry

    ok(page.ghosts, 'no ghosts')
    ok(corporal.items.length - corporal.ghosts > 0, 'no replacement')

    // todo: how is this not a race condition? I'm writing to the log, but I've
    // not updated the pivot page, not rewritten during `deleteGhosts`.
    this.splice(page, 0, 1, this.splice(corporal, corporal.ghosts, 1))
    page.ghosts = 0

    var item = this.splice(pivot.page, pivot.index, 1).shift()
    item.key = page.items[0].key
    item.heft = page.items[0].heft
    this.splice(pivot.page, pivot.index, 0, item)

    async(function () {
        entry = this.journal.leaf.open(this._filename(page.address, 0), page.position, page)
        entry.ready(async())
    }, function () {
    // todo: close on failure.
        entry.close('entry', async())
    }, function () {
        return []
    })
}))

prototype(Sheaf, 'deleteGhost', cadence(function (async, key) {
    var locker = this.createLocker(),
        descents = [],
        pivot, leaf, fd
    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(pivot = new Descent(this, locker))
        pivot.descend(pivot.key(key), pivot.found([key]), async())
    }, function () {
        pivot.upgrade(async())
    }, function () {
        descents.push(leaf = pivot.fork())

        leaf.descend(leaf.key(key), leaf.leaf, async())
    }, function () {
        this.exorcise(pivot, leaf.page, leaf.page, async())
    })
}))

prototype(Sheaf, 'mergePages', cadence(function (async, key, leftKey, stopper, merger, ghostly) {
    var locker = this.createLocker(),
        descents = [], singles = { left: [], right: [] }, parents = {}, pages = {},
        ancestor, pivot, empties, ghosted, designation

    function createSingleUnlocker (singles) {
        ok(singles != null, 'null singles')
        return function (parent, child) {
            if (child.items.length == 1) {
                if (singles.length == 0) singles.push(parent)
                singles.push(child)
            } else if (singles.length) {
                singles.forEach(function (page) { locker.unlock(page) })
                singles.length = 0
            } else {
                locker.unlock(parent)
            }
        }
    }

    var keys = [ key ]
    if (leftKey) keys.push(leftKey)

    async(function () {
        async([function () {
            descents.forEach(function (descent) { locker.unlock(descent.page) })
            ! [ 'left', 'right' ].forEach(function (direction) {
                if (singles[direction].length) {
                    singles[direction].forEach(function (page) { locker.unlock(page) })
                } else {
                    locker.unlock(parents[direction].page)
                }
            })
            locker.dispose()
        }], function () {
            descents.push(pivot = new Descent(this, locker))
            pivot.descend(pivot.key(key), pivot.found(keys), async())
        }, function () {
            var found = pivot.page.items[pivot.index].key
            if (this.comparator(found, keys[0]) == 0) {
                pivot.upgrade(async())
            } else {
                async(function () { // left above right
                    pivot.upgrade(async())
                }, function () {
                    ghosted = { page: pivot.page, index: pivot.index }
                    descents.push(pivot = pivot.fork())
                    keys.pop()
                    pivot.descend(pivot.key(key), pivot.found(keys), async())
                })
            }
        }, function () {
            parents.right = pivot.fork()
            parents.right.unlocker = createSingleUnlocker(singles.right)
            parents.right.descend(parents.right.key(key), stopper(parents.right), async())
        }, function () {
            parents.left = pivot.fork()
            parents.left.index--
            parents.left.unlocker = createSingleUnlocker(singles.left)
            parents.left.descend(parents.left.right,
                                 parents.left.level(parents.right.depth),
                                 async())
        }, function () {
            if (singles.right.length) {
                ancestor = singles.right[0]
            } else {
                ancestor = parents.right.page
            }

            if (leftKey && !ghosted) {
                if (singles.left.length) {
                    ghosted = { page: singles.left[0], index: parents.left.indexes[singles.left[0].address] }
                } else {
                    ghosted = { page: parents.left.page, index: parents.left.index }
                    ok(parents.left.index == parents.left.indexes[parents.left.page.address], 'TODO: ok to replace the above')
                }
            }

            descents.push(pages.left = parents.left.fork())
            pages.left.descend(pages.left.left, pages.left.level(parents.left.depth + 1), async())
        }, function () {
            descents.push(pages.right = parents.right.fork())
            pages.right.descend(pages.right.left, pages.right.level(parents.right.depth + 1), async())
        }, function () {
            merger.call(this, pages, ghosted, async())
        }, function (dirty) {
            if (!dirty) return [ async ]
        }, function () {
            this._rename(pages.right.page, 0, '', '.unlink', async())
        }, function () {
            var index = parents.right.indexes[ancestor.address]

            designation = this.splice(ancestor, index, 1).shift()

            if (pivot.page.address != ancestor.address) {
                ok(!index, 'expected ancestor to be removed from zero index')
                ok(ancestor.items[index], 'expected ancestor to have right sibling')
                designation = this.splice(ancestor, index, 1).shift()
                var hoist = this.splice(pivot.page, pivot.index, 1).shift()
                this.splice(pivot.page, pivot.index, 0, {
                    key: designation.key,
                    address: hoist.address,
                    heft: designation.heft
                })
                this.splice(ancestor, index, 0, { address: designation.address, heft: 0 })
            } else{
                ok(index, 'expected ancestor to be non-zero')
            }

            this.writeBranch(ancestor, '.pending', async())
        }, function () {
            async(function (page) {
                this._rename(page, 0, '', '.unlink', async())
            })(singles.right.slice(1))
        }, function () {
            this._rename(ancestor, 0, '.pending', '.commit', async())
        }, function () {
            async(function (page) {
                this._unlink(page, 0, '.unlink', async())
            })(singles.right.slice(1))
        }, function () {
            this.replace(pages.left.page, '.replace', async())
        }, function () {
            this._unlink(pages.right.page, 0, '.unlink', async())
        }, function () {
            this.replace(ancestor, '.commit', async())
        })
    }, function () {
        if (ancestor.address == 0) {
            if (ancestor.items.length == 1 && !(ancestor.items[0].address % 2)) {
                this.fillRoot(async())
            }
        } else {
            this.chooseBranchesToMerge(designation.key, ancestor.address, async())
        }
    })
}))

prototype(Sheaf, 'mergeLeaves', function (key, leftKey, unbalanced, ghostly, callback) {
    function stopper (descent) { return descent.penultimate }

    var merger = cadence(function (async, leaves, ghosted) {
        ok(leftKey == null ||
           this.comparator(leftKey, leaves.left.page.items[0].key)  == 0,
           'left key is not as expected')

        var left = (leaves.left.page.items.length - leaves.left.page.ghosts)
        var right = (leaves.right.page.items.length - leaves.right.page.ghosts)

        this.unbalanced(leaves.left.page, true)

        var index
        if (left + right > this.options.leafSize) {
            if (unbalanced[leaves.left.page.address]) {
                this.unbalanced(leaves.left.page, true)
            }
            if (unbalanced[leaves.right.page.address]) {
                this.unbalanced(leaves.right.page, true)
            }
            return [ false ]
        } else {
            async(function () {
                if (ghostly && left + right) {
                    if (left) {
                        this.exorcise(ghosted, leaves.left.page, leaves.left.page, async())
                    } else {
                        this.exorcise(ghosted, leaves.left.page, leaves.right.page, async())
                    }
                }
            }, function () {
                leaves.left.page.right = leaves.right.page.right
                var ghosts = leaves.right.page.ghosts
                async(function (index) {
                    index += ghosts
                    var item = leaves.right.page.items[index]
                    this.splice(leaves.left.page, leaves.left.page.items.length, 0, item)
                })(leaves.right.page.items.length - leaves.right.page.ghosts)
            }, function () {
                this.splice(leaves.right.page, 0, leaves.right.page.items.length)

                this.rewriteLeaf(leaves.left.page, '.replace', async())
            }, function () {
                return [ true ]
            })
        }
    })

    this.mergePages(key, leftKey, stopper, merger, ghostly, callback)
})

prototype(Sheaf, 'chooseBranchesToMerge', cadence(function (async, key, address) {
    var locker = this.createLocker(),
        descents = [],
        designator, choice, lesser, greater, center

    var goToPage = cadence(function (async, descent, address, direction) {
        async(function () {
            descents.push(descent)
            descent.descend(descent.key(key), descent.address(address), async())
        }, function () {
            descent.index += direction == 'left' ? 1 : -1
                                        // ^^^ This ain't broke.
            descent.descend(descent[direction], descent.level(center.depth), async())
        })
    })

    var choose = async(function () {
        async([function () {
            descents.forEach(function (descent) { locker.unlock(descent.page) })
            locker.dispose()
        }], function () {
            descents.push(center = new Descent(this, locker))
            center.descend(center.key(key), center.address(address), async())
        }, function () {
            if (center.lesser != null) {
                goToPage(lesser = new Descent(this, locker), center.lesser, 'right', async())
            }
        }, function () {
            if (center.greater != null) {
                goToPage(greater = new Descent(this, locker), center.greater, 'left', async())
            }
        }, function () {
            if (lesser && lesser.page.items.length + center.page.items.length <= this.options.branchSize) {
                choice = center
            } else if (greater && greater.page.items.length + center.page.items.length <= this.options.branchSize) {
                choice = greater
            }

            if (choice) {
                descents.push(designator = choice.fork())
                designator.index = 0
                designator.descend(designator.left, designator.leaf, async())
            } else {
                return [ choose ]
            }
        }, function () {
            return designator.page.items[0]
        })
    }, function (entry) {
        this.mergeBranches(entry.key, entry.heft, choice.page.address, async())
    })
}))

prototype(Sheaf, 'mergeBranches', function (key, heft, address, callback) {
    function stopper (descent) {
        return descent.child(address)
    }

    var merger = cadence(function (async, pages, ghosted) {
        ok(address == pages.right.page.address, 'unexpected address')

        var cut = this.splice(pages.right.page, 0, pages.right.page.items.length)

        cut[0].key = key
        cut[0].heft = heft

        this.splice(pages.left.page, pages.left.page.items.length, 0, cut)

        async(function () {
            this.writeBranch(pages.left.page, '.replace', async())
        }, function () {
            return [ true ]
        })
    })

    this.mergePages(key, null, stopper, merger, false, callback)
})

prototype(Sheaf, 'fillRoot', cadence(function (async) {
    var locker = this.createLocker(), descents = [], root, child

    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(root = new Descent(this, locker))
        root.exclude()
        root.descend(root.left, root.level(0), async())
    }, function () {
        descents.push(child = root.fork())
        child.descend(child.left, child.level(1), async())
    }, function () {
        var cut
        ok(root.page.items.length == 1, 'only one address expected')

        this.splice(root.page, 0, root.page.items.length)

        cut = this.splice(child.page, 0, child.page.items.length)

        this.splice(root.page, root.page.items.length, 0, cut)

        this.writeBranch(root.page, '.pending', async())
    }, function () {
        this._rename(child.page, 0, '', '.unlink', async())
    }, function () {
        this._rename(root.page, 0, '.pending', '.commit', async())
    }, function () {
        this._unlink(child.page, 0, '.unlink', async())
    }, function () {
        this.replace(root.page, '.commit', async())
    })
}))

function Sheaf (options) {
    this.fs = options.fs || require('fs')
    this.nextAddress = 0
    this.directory = options.directory
    this.journal = {
        branch: new Journalist({ stage: 'entry' }).createJournal(),
        leaf: new Journalist({
            stage: 'entry'
        }).createJournal()
    }
    this.journalist = new Journalist({
        count: options.fileHandleCount || 64,
        stage: options.writeStage || 'entry',
        cache: options.jouralistCache || new Cache()
    })
    this.cache = options.cache || (new Cache)
    this.options = options
    this.tracer = options.tracer || function () { arguments[2]() }
    this.sequester = options.sequester || require('sequester')
    this.extractor = options.extractor || extract
    this.comparator = options.comparator || compare
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

Sheaf.prototype.readEntry = function (buffer, isKey) {
    for (var count = 2, i = 0, I = buffer.length; i < I && count; i++) {
        if (buffer[i] == 0x20) count--
    }
    for (count = 1; i < I && count; i++) {
        if (buffer[i] == 0x20 || buffer[i] == 0x0a) count--
    }
    ok(!count, 'corrupt line: could not find end of line header')
    var fields = buffer.toString('utf8', 0, i - 1).split(' ')
    var hash = this.checksum(), body, length
    hash.update(fields[2])
    if (buffer[i - 1] == 0x20) {
        body = buffer.slice(i, buffer.length - 1)
        length = body.length
        hash.update(body)
    }
    var digest = hash.digest('hex')
    ok(fields[1] == '-' || digest == fields[1], 'corrupt line: invalid checksum')
    if (buffer[i - 1] == 0x20) {
        body = this.deserialize(body, isKey)
    }
    var entry = { length: length, header: JSON.parse(fields[2]), body: body }
    ok(entry.header.every(function (n) { return typeof n == 'number' }), 'header values must be numbers')
    return entry
}

Sheaf.prototype._filename = function (address, rotation, suffix) {
    suffix || (suffix = '')
    return path.join(this.directory, address + '.' + rotation + suffix)
}

prototype(Sheaf, 'replace', cadence(function (async, page, suffix) {
    var replacement = this._filename(page.address, 0, suffix),
        permanent = this._filename(page.address, 0)

    async(function () {
        this.fs.stat(replacement, async())
    }, function (stat) {
        ok(stat.isFile(), 'is not a file')
        async([function () {
            this.fs.unlink(permanent, async())
        }, /^ENOENT$/, function () {
            // todo: regex only is a catch and swallow?
        }])
    }, function (ror) {
        this.fs.rename(replacement, permanent, async())
    })
}))

prototype(Sheaf, '_rename', function (page, rotation, from, to, callback) {
    this.fs.rename(
        this._filename(page.address, rotation, from),
        this._filename(page.address, rotation, to),
        callback)
})

prototype(Sheaf, '_unlink', function (page, rotation, suffix, callback) {
    this.fs.unlink(this._filename(page.address, rotation, suffix), callback)
})

Sheaf.prototype.heft = function (page, s) {
    this.magazine.get(page.address).adjustHeft(s)
}

Sheaf.prototype.createLeaf = function (override) {
    return this.createPage({
        loaders: {},
        entries: 0,
        ghosts: 0,
        items: [],
        right: 0,
        queue: this.sequester.createQueue()
    }, override, 0)
}

prototype(Sheaf, 'writeEntry', cadence(function (async, options) {
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

    buffer = new Buffer(length)
    buffer.write(String(length) + ' ' + line)
    if (options.body != null) {
        body.copy(buffer, buffer.length - 1 - body.length)
    }
    buffer[length - 1] = 0x0A

    var position = options.page.position

    async(function () {
        options.out.write(buffer, async())
    }, function () {
        options.page.position += length
        return [ position, length, body && body.length ]
    })
}))

prototype(Sheaf, 'writeInsert', function (out, page, index, record, callback) {
    var header = [ ++page.entries, index + 1 ]
    this.writeEntry({ out: out, page: page, header: header, body: record, type: 'insert' }, callback)
})

prototype(Sheaf, 'writeDelete', function (out, page, index, callback) {
    var header = [ ++page.entries, -(index + 1) ]
    this.writeEntry({ out: out, page: page, header: header, type: 'delete' }, callback)
})

prototype(Sheaf, 'writeHeader', function (out, page, callback) {
    var header = [ 0, ++page.entries, page.right ]
    this.writeEntry({ out: out, page: page, header: header, type: 'header' }, callback)
})

prototype(Sheaf, 'io', cadence(function (async, direction, filename) {
    async(function () {
        this.fs.open(filename, direction[0], async())
    }, function (fd) {
        async(function () {
            this.fs.fstat(fd, async())
        }, function (stat) {
            var fs = this.fs, io = cadence(function (async, buffer, position) {
                var offset = 0

                var length = stat.size - position
                var slice = length < buffer.length ? buffer.slice(0, length) : buffer

                var loop = async(function (count) {
                    if (count < slice.length - offset) {
                        offset += count
                        fs[direction](fd, slice, offset, slice.length - offset, position + offset, async())
                    } else {
                        return [ loop, slice, position ]
                    }
                })(null, 0)
            })
            return [ fd, stat, io ]
        })
    })
}))

Sheaf.prototype.readHeader = function (entry) {
    var header = entry.header
    return {
        entry:      header[0],
        index:      header[1],
        address:    header[2]
    }
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

prototype(Sheaf, 'readLeaf', cadence(function (async, page) {
    async(function () {
        this.io('read', this._filename(page.address, 0), async())
    }, function (fd, stat, read) {
        async(function () {
            page.entries = 0
            page.ghosts = 0
            return [ page, 0 ]
        }, function (page, position) {
            this.replay(fd, stat, read, page, position, async())
        }, function () {
            return [ page ]
        })
    })
}))

prototype(Sheaf, 'replay', cadence(function (async, fd, stat, read, page, position) {
    var leaf = !!(page.address % 2),
        seen = {},
        buffer = new Buffer(this.options.readLeafStartLength || 1024),
        footer, length

    // todo: really want to register a cleanup without an indent.
    async([function () {
        this.fs.close(fd, async())
    }], function () {
        page.position = 0
        var loop = async(function (buffer, position) {
            read(buffer, position, async())
        }, function (slice, start) {
            for (var offset = 0, i = 0, I = slice.length; i < I; i++) {
                ok(!footer, 'data beyond footer')
                if (slice[i] == 0x20) {
                    var sip = slice.toString('utf8', offset, i)
                    length = parseInt(sip)
                    ok(String(length).length == sip.length, 'invalid length')
                    if (offset + length > slice.length) {
                        break
                    }
                    var position = start + offset
                    ok(length)
                    page.position += length
                    var entry = this.readEntry(slice.slice(offset, offset + length), !leaf)
                    var header = this.readHeader(entry)
                    if (entry.header[0]) {
                        ok(header.entry == ++page.entries, 'entry count is off')
                        var index = header.index
                        if (leaf) {
                            if (index > 0) {
                                seen[position] = true
                                this.splice(page, index - 1, 0, {
                                    key: this.extractor(entry.body),
                                    record: entry.body,
                                    heft: length
                                })
                            } else if (~index == 0 && page.address != 1) {
                                ok(!page.ghosts, 'double ghosts')
                                page.ghosts++
                            } else if (index < 0) {
                                this.splice(page, -(index + 1), 1)
                            }
                        } else {
                            var address = header.address, key = null, heft = 0
                            if (index - 1) {
                                key = entry.body
                                heft = length
                            }
                            this.splice(page, index - 1, 0, {
                                key: key, address: address, heft: heft
                            })
                        }
                    } else {
                        page.right = entry.header[2]
                        page.entries++
                    }
                    i = offset = offset + length
                }
            }

            if (start + buffer.length < stat.size) {
                if (offset == 0) {
                    buffer = new Buffer(buffer.length * 2)
                    read(buffer, start, async())
                } else {
                    read(buffer, start + offset, async())
                }
            } else {
                return [ loop, page, footer ]
            }
        })(null, buffer, position)
    })
}))

prototype(Sheaf, 'rewriteLeaf', cadence(function (async, page, suffix) {
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
        page.position = 0
        page.entries = 0

        var items = this.splice(page, 0, page.items.length)

        async(function () {
            this.writeHeader(out, page, async())
        }, function () {
            async(function (item) {
                async(function () {
                    this.writeInsert(out, page, index++, item.record, async())
                }, function (position, length) {
                    this.splice(page, page.items.length, 0, item)
                })
            })(items)
        })
    }, function () {
        out.close('entry', async())
    })
}))

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

prototype(Sheaf, 'writeBranch', cadence(function (async, page, suffix) {
    var items = page.items, out

    ok(items[0].key == null, 'key of first item must be null')
    ok(items[0].heft == 0, 'heft of first item must be zero')
    ok(items.slice(1).every(function (item) { return item.key != null }), 'null keys')

    async(function () {
        page.entries = 0
        page.position = 0

        out = this.journal.branch.open(this._filename(page.address, 0, suffix), 0, page)
        out.ready(async())
    }, [function () {
        out.scram(async())
    }], function () {
        async(function (item) {
            var key = page.entries ? item.key : null
            page.entries++
            var header = [ page.entries, page.entries, item.address ]
            this.writeEntry({
                out: out,
                page: page,
                header: header,
                body: key,
                isKey: true
            }, async())
        })(page.items)
    }, function () {
        out.close('entry', async())
    })
}))

prototype(Sheaf, 'readBranch', cadence(function (async, page) {
    async(function () {
        this.io('read', this._filename(page.address, 0), async())
    }, function (fd, stat, read) {
        this.replay(fd, stat, read, page, 0, async())
    })
}))

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

function Strata (options) {
    this.sheaf = new Sheaf(options)
}

Strata.prototype.__defineGetter__('size', function () {
    return this.sheaf.magazine.heft
})

Strata.prototype.__defineGetter__('nextAddress', function () {
    return this.sheaf.nextAddress
})

// to user land
prototype(Strata, 'create', cadence(function (async) {
    this.sheaf.createMagazine()

    var locker = this.sheaf.createLocker(), count = 0, root, leaf, journal

    async([function () {
        locker.dispose()
    }], function () {
        this.sheaf.fs.stat(this.sheaf.directory, async())
    }, function (stat) {
        ok(stat.isDirectory(), 'database ' + this.sheaf.directory + ' is not a directory.')
        this.sheaf.fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
              'database ' + this.sheaf.directory + ' is not empty.')

        root = locker.encache(this.sheaf.createBranch({ penultimate: true }))
        leaf = locker.encache(this.sheaf.createLeaf({}))
        this.sheaf.splice(root, 0, 0, { address: leaf.address, heft: 0 })

        this.sheaf.writeBranch(root, '.replace', async())
    }, [function () {
        locker.unlock(root)
    }], function () {
        this.sheaf.rewriteLeaf(leaf, '.replace', async())
    }, [function () {
        locker.unlock(leaf)
    }], function () {
        this.sheaf.replace(root, '.replace', async())
    }, function branchReplaced () {
        this.sheaf.replace(leaf, '.replace', async())
    })
}))

// to user land
prototype(Strata, 'open', cadence(function (async) {
    this.sheaf.createMagazine()

    // todo: instead of rescue, you might try/catch the parts that you know
    // are likely to cause errors and raise an error of a Strata type.

    // todo: or you might need some way to catch a callback error. Maybe an
    // outer most catch block?

    // todo: or if you're using Cadence, doesn't the callback get wrapped
    // anyway?
    async(function () {
        this.sheaf.fs.stat(this.sheaf.directory, async())
    }, function stat (error, stat) {
        this.sheaf.fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        files.forEach(function (file) {
            if (/^\d+\.\d+$/.test(file)) {
                this.sheaf.nextAddress = Math.max(+(file.split('.').shift()) + 1, this.sheaf.nextAddress)
            }
        }, this)
    })
}))

// to user land
prototype(Strata, 'close', cadence(function (async) {
    var cartridge = this.sheaf.magazine.get(-2), lock = cartridge.value.page.lock
    async(function () {
        this.sheaf.createJournal().close('tree', async())
    }, function () {
        lock.unlock()
        // todo
        lock.dispose()

        cartridge.release()

        var purge = this.sheaf.magazine.purge()
        while (purge.cartridge) {
            purge.cartridge.remove()
            purge.next()
        }
        purge.release()

        ok(!this.sheaf.magazine.count, 'pages still held by cache')
    })
}))

prototype(Strata, 'left', function (descents, exclusive, callback) {
    this.toLeaf(descents[0].left, descents, null, exclusive, callback)
})

prototype(Strata, 'right', function (descents, exclusive, callback) {
    this.toLeaf(descents[0].right, descents, null, exclusive, callback)
})

prototype(Strata, 'key', function (key) {
    return function (descents, exclusive, callback) {
        this.toLeaf(descents[0].key(key), descents, null, exclusive, callback)
    }
})

prototype(Strata, 'leftOf', function (key) {
    return cadence(function (async, descents, exclusive) {
        var conditions = [ descents[0].leaf, descents[0].found([key]) ]
        async(function () {
            descents[0].descend(descents[0].key(key), function () {
                return conditions.some(function (condition) {
                    return condition.call(descents[0])
                })
            }, async())
        }, function (page, index) {
            if (descents[0].page.address % 2) {
                return [ new Cursor(this.sheaf, this.sheaf.createJournal(), descents, false, key) ]
            } else {
                descents[0].index--
                this.toLeaf(descents[0].right, descents, null, exclusive, async())
            }
        })
    })
})

prototype(Strata, 'toLeaf', cadence(function (async, sought, descents, key, exclusive) {
    async(function () {
        descents[0].descend(sought, descents[0].penultimate, async())
    }, function () {
        if (exclusive) descents[0].exclude()
        descents[0].descend(sought, descents[0].leaf, async())
    }, function () {
        return [ new Cursor(this.sheaf, this.sheaf.createJournal(), descents, exclusive, key) ]
    })
}))

// to user land
prototype(Strata, 'cursor', cadence(function (async, key, exclusive) {
    var descents = [ new Descent(this.sheaf, this.sheaf.createLocker()) ]
    async([function () {
        if (descents.length) {
            descents[0].locker.unlock(descents[0].page)
            descents[0].locker.dispose()
        }
    }], function () {
        if  (typeof key == 'function') {
            key.call(this, descents, exclusive, async())
        } else {
            this.toLeaf(descents[0].key(key), descents, key, exclusive, async())
        }
    }, function (cursor) {
        return [ cursor ]
    })
}))

prototype(Strata, 'iterator', function (key, callback) {
    this.cursor(key, false, callback)
})

prototype(Strata, 'mutator', function (key, callback) {
    this.cursor(key, true, callback)
})

// to user land
prototype(Strata, 'balance', function (callback) {
    this.sheaf.balance(callback)
})

// to user land
prototype(Strata, 'vivify', cadence(function (async) {
    var locker = this.sheaf.createLocker(), root

    function record (item) {
        return { address: item.address }
    }

    async(function () {
        locker.lock(0, false, async())
    }, function (page) {
        async([function () {
            locker.unlock(page)
            locker.dispose()
        }], function () {
            expand.call(this, page, root = page.items.map(record), 0, async())
        })
    })

    var expand = cadence(function (async, parent, pages, index) {
        var block = async(function () {
            if (index < pages.length) {
                var address = pages[index].address
                locker.lock(address, false, async(async)([function (page) { locker.unlock(page) }]))
            } else {
                return [ block, pages ]
            }
        }, function (page) {
            if (page.address % 2 == 0) {
                async(function () {
                    pages[index].children = page.items.map(record)
                    if (index) {
                        pages[index].key = parent.items[index].key
                    }
                    expand.call(this, page, pages[index].children, 0, async())
                }, function () {
                    expand.call(this, parent, pages, index + 1, async())
                })
            } else {
                async(function () {
                    pages[index].children = []
                    pages[index].ghosts = page.ghosts

                    async(function (recordIndex) {
                        pages[index].children.push(page.items[recordIndex].record)
                    })(page.items.length)
                }, function () {
                    expand.call(this, parent, pages, index + 1, async())
                })
            }
        })(1)
    })
}))

Strata.prototype.purge = function (downTo) {
    var purge = this.sheaf.magazine.purge()
    while (purge.cartridge && this.sheaf.magazine.heft > downTo) {
        purge.cartridge.remove()
        purge.next()
    }
    purge.release()
}

Strata.prototype.__defineGetter__('balanced', function () {
    return ! Object.keys(this.sheaf.lengths).length
})

module.exports = Strata
