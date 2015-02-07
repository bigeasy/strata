var ok = require('assert').ok
var path = require('path')

var cadence = require('cadence/redux')
var prototype = require('pointcut').prototype

require('cadence/loops')

var Journalist = require('journalist')
var Cache = require('magazine')

var extend = require('./extend')

var Descent = require('./descent')
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

prototype(Sheaf, 'unbalanced', function (page, force) {
    if (force) {
        this.lengths[page.address] = this.options.leafSize
    } else if (this.lengths[page.address] == null) {
        this.lengths[page.address] = page.items.length - page.ghosts
    }
})

prototype(Sheaf, '_node', cadence(function (async, locker, page) {
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
            rightAddress: page.right.address,
            length: page.items.length - page.ghosts
        }
        this.ordered[node.address] = node
        if (page.ghosts) {
            this.ghosts[node.address] = node
        }
        return [ node ]
    })
}))

prototype(Sheaf, '_nodify', cadence(function (async, locker, page) {
    async(function () {
        this._node(locker, page, async())
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
        async.forEach(function (address) {
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
            async.forEach(function (operation) {
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

prototype(Sheaf, 'splitLeafAndUnlock', cadence(function (async, address, key, ghosts) {
    var locker = this.createLocker(),
        script = new Script(this),
        descents = [], replacements = [], encached = [],
        completed = 0,
        penultimate, leaf, split, pages, page,
        records, remainder, right, index, offset, length

    var splitter = async([function () {
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
            return [ splitter, false ]
        }
    }, function () {
        pages = Math.ceil(split.items.length / this.options.leafSize)
        records = Math.floor(split.items.length / pages)
        remainder = split.items.length % pages

        right = split.right

        offset = split.items.length

        var splits = 0
        var loop = async(function () {
            if (splits++ == pages - 1) return [ loop ]
            page = locker.encache(this.createLeaf({ loaded: true }))
            encached.push(page)

            page.right = right

            length = remainder-- > 0 ? records + 1 : records
            offset = split.items.length - length
            index = offset

            this.splice(penultimate.page, penultimate.index + 1, 0, {
                key: split.items[offset].key,
                heft: this.serialize(split.items[offset].key, true).length,
                address: page.address
            })

            for (var i = 0; i < length; i++) {
                var item = split.items[index]

                ok(index < split.items.length)

                this.splice(page, page.items.length, 0, item)

                index++
            }

            right = {
                address: page.address,
                key: page.items[0].key
            }
        }, function () {
            this.splice(split, offset, length)
            script.rewriteLeaf(page)
        })()
    }, function () {
        split.right = right
        script.rewriteLeaf(split)
        script.writeBranch(penultimate.page)
        script.commit(async())
    }, function () {
        this.unbalanced(leaf.page, true)
        this.unbalanced(page, true)
        return [ splitter, true, penultimate.page, encached[0].items[0].key ]
    })()
}))

prototype(Sheaf, 'splitLeaf', cadence(function (async, address, key, ghosts) {
    async(function () {
        this.splitLeafAndUnlock(address, key, ghosts, async())
    }, function (split, penultimate, partition) {
        if (split) {
            this.shouldSplitBranch(penultimate, partition, async())
        }
    })
}))

prototype(Sheaf, 'splitBranchAndUnlock', cadence(function (async, address, key) {
    var locker = this.createLocker(),
        script = new Script(this),
        descents = [],
        children = [],
        encached = [],
        parent, full, split, pages,
        records, remainder, offset,
        unwritten, pending

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

        children.unshift(full.page)
        children.forEach(function (page) {
            script.writeBranch(page)
        })
        script.writeBranch(parent.page)
        script.commit(async())
    }, function () {
        return [ parent.page, key ]
    })
}))

prototype(Sheaf, 'splitBranch', cadence(function (async, address, key) {
    async(function () {
        this.splitBranchAndUnlock(address, key, async())
    }, function (parent, key) {
        this.shouldSplitBranch(parent, key, async())
    })
}))

prototype(Sheaf, 'drainRootAndUnlock', cadence(function (async) {
    var locker = this.createLocker(),
        script = new Script(this),
        children = [], locks = [],
        root, pages, records, remainder

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

        children.forEach(function (page) {
            script.writeBranch(page)
        })
        script.writeBranch(root)
        script.commit(async())
    }, function () {
        return [ root ]
    })
}))

prototype(Sheaf, 'drainRoot', cadence(function (async) {
    async(function () {
        this.drainRootAndUnlock(async())
    }, function (root) {
        if (root.items.length > this.options.branchSize) this.drainRoot(async())
    })
}))

Sheaf.prototype.exorcise2 = function (pivot, page, corporal) {
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
}

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
        script = new Script(this),
        descents = [],
        pivot, leaf, reference
    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(pivot = new Descent(this, locker))
        pivot.descend(pivot.key(key), pivot.found([key]), async())
    }, function () {
        pivot.upgrade(async())
    }, function () {
        if (pivot.index != 0) {
            descents.push(reference = pivot.fork())
            reference.index--
            reference.descend(reference.right, reference.leaf, async())
        }
    }, function () {
        descents.push(leaf = pivot.fork())
        leaf.descend(leaf.key(key), leaf.leaf, async())
    }, function () {
        this.exorcise2(pivot, leaf.page, leaf.page)
        script.rotate(leaf.page)
        if (reference) {
            reference.page.right.key = leaf.page.items[0].key
            script.rotate(reference.page)
        }
        script.writeBranch(pivot.page)
        script.commit(async())
    }, function () {
        return [ leaf.page.items[0].key ]
    })
}))

prototype(Sheaf, 'referring', cadence(function (async, leftKey, descents, pivot, pages) {
    var referring
    if (leftKey != null && pages.referring == null) {
        descents.push(referring = pages.referring = pivot.fork())
        async(function () {
            var key = referring.page.items[referring.index].key
            if (this.comparator(leftKey, key) !== 0) {
                referring.index--
                referring.descend(referring.key(leftKey), referring.found([leftKey]), async())
            }
        }, function () {
            var key = referring.page.items[referring.index].key
            ok(this.comparator(leftKey, key) === 0, 'cannot find left key')
            referring.index--
            referring.descend(referring.right, referring.leaf, async())
        })
    }
}))

prototype(Sheaf, 'mergePagesAndUnlock', cadence(function (async, key, leftKey, stopper, merger, ghostly) {
    var locker = this.createLocker(),
        script = new Script(this),
        descents = [],
        singles = { left: [], right: [] }, parents = {}, pages = {},
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

    var merge = async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        ! [ 'left', 'right' ].forEach(function (direction) {
            // todo: use `pages` array, these conditions are tricky,
            // `parents[direction]` may not yet exist.
            if (singles[direction].length) {
                singles[direction].forEach(function (page) { locker.unlock(page) })
            } else if (parents[direction]) {
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
                this.referring(leftKey, descents, pivot, pages, async())
            }, function () {
                ghosted = { page: pivot.page, index: pivot.index }
                descents.push(pivot = pivot.fork())
                keys.pop()
                pivot.descend(pivot.key(key), pivot.found(keys), async())
            })
        }
    }, function () {
        this.referring(leftKey, descents, pivot, pages, async())
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
        merger.call(this, script, pages, ghosted, async())
    }, function (dirty) {
        if (!dirty) return [ merge, false ]
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

        script.unlink(pages.right.page)
        script.writeBranch(ancestor)
        singles.right.slice(1).forEach(function (page) {
            script.unlink(page)
        })
        script.commit(async())
    }, function () {
        return [ merge, true, ancestor, designation.key ]
    })()
}))

prototype(Sheaf, 'mergePages', cadence(function (async, key, leftKey, stopper, merger, ghostly) {
    async(function () {
        this.mergePagesAndUnlock(key, leftKey, stopper, merger, ghostly, async())
    }, function (merged, ancestor, designation) {
        if (merged) {
            if (ancestor.address == 0) {
                if (ancestor.items.length == 1 && !(ancestor.items[0].address % 2)) {
                    this.fillRoot(async())
                }
            } else {
                this.chooseBranchesToMerge(designation, ancestor.address, async())
            }
        }
    })
}))

prototype(Sheaf, 'mergeLeaves', function (key, leftKey, unbalanced, ghostly, callback) {
    function stopper (descent) { return descent.penultimate }

    var merger = cadence(function (async, script, leaves, ghosted) {
        ok(leftKey == null ||
           this.comparator(leftKey, leaves.left.page.items[0].key) == 0,
           'left key is not as expected')
        ok(leftKey == null || leaves.referring != null, 'no referring page')
        ok(leftKey != null || leaves.referring == null, 'referring page when leftmost')

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
                var count = leaves.right.page.items.length - leaves.right.page.ghosts
                var index = 0
                var loop = async(function () {
                    if (index == count) return [ loop ]
                    var item = leaves.right.page.items[index + ghosts]
                    this.splice(leaves.left.page, leaves.left.page.items.length, 0, item)
                    index++
                })()
            }, function () {
                this.splice(leaves.right.page, 0, leaves.right.page.items.length)
                if (leftKey) {
                    leaves.referring.page.right.key = leaves.left.page.items[0].key
                    script.rotate(leaves.referring.page)
                }
                script.rewriteLeaf(leaves.left.page)
            }, function () {
                return [ true ]
            })
        }
    })

    this.mergePages(key, leftKey, stopper, merger, ghostly, callback)
})

prototype(Sheaf, 'chooseBranchesToMergeAndUnlock', cadence(function (async, key, address) {
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
                return [ choose , false ]
            }
        }, function () {
            var item = designator.page.items[0]
            return [ choose, true, item.key, item.heft, choice.page.address ]
        })
    })()
}))

prototype(Sheaf, 'chooseBranchesToMerge', cadence(function (async, key, address) {
    async(function () {
        this.chooseBranchesToMergeAndUnlock(key, address, async())
    }, function (merge, key, heft, address) {
        if (merge) {
            this.mergeBranches(key, heft, address, async())
        }
    })
}))

prototype(Sheaf, 'mergeBranches', function (key, heft, address, callback) {
    function stopper (descent) {
        return descent.child(address)
    }

    var merger = cadence(function (async, script, pages, ghosted) {
        ok(address == pages.right.page.address, 'unexpected address')

        var cut = this.splice(pages.right.page, 0, pages.right.page.items.length)

        cut[0].key = key
        cut[0].heft = heft

        this.splice(pages.left.page, pages.left.page.items.length, 0, cut)

        script.writeBranch(pages.left.page)

        return true
    })

    this.mergePages(key, null, stopper, merger, false, callback)
})

prototype(Sheaf, 'fillRoot', cadence(function (async) {
    var locker = this.createLocker(), script = new Script(this), descents = [], root, child

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

        script.writeBranch(root.page)
        script.unlink(child.page)
        script.commit(async())
    })
}))

Sheaf.prototype.filename2 = function (page, suffix) {
    return this._filename(page.address, page.rotation, suffix)
}

Sheaf.prototype._filename = function (address, rotation, suffix) {
    suffix || (suffix = '')
    return path.join(this.directory, address + '.' + rotation + suffix)
}

prototype(Sheaf, 'replace', cadence(function (async, page, suffix) {
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

prototype(Sheaf, 'writeBranch', cadence(function (async, page, file) {
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

module.exports = Sheaf
