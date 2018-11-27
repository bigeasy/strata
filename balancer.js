var ok = require('assert').ok
var cadence = require('cadence')
var Script = require('./script')
var Descent = require('./descent')

function Balancer (sheaf, logger) {
    this.sheaf = sheaf
    this.logger = logger
}

Balancer.prototype._node = cadence(function (async, locker, page) {
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
})

Balancer.prototype._nodify = cadence(function (async, locker, page) {
    async(function () {
        this._node(locker, page, async())
    }, function (node) {
        async(function () {
            this.sheaf.tracer('reference', {}, async())
        }, function () {
            return node
        })
    })
})

Balancer.prototype.balance = cadence(function balance (async, sheaf) {
    var locker = this.sheaf.createLocker(), operations = [], address, length

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
                var descent = new Descent(this.sheaf, locker)
                async(function () {
                    descent.descend(descent.key(node.key), descent.found([node.key]), async())
                }, function () {
                    descent.setIndex(descent.index - 1)
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

    ok(!this.sheaf.balancing, 'already balancing')

    var lengths = this.sheaf.lengths, addresses = Object.keys(lengths)
    if (addresses.length == 0) {
        return [ async, true ]
    } else {
        this.sheaf.lengths = {}
        this.operations = []
        this.ordered = {}
        this.ghosts = {}
        this.sheaf.balancing = true
    }

    async(function () {
        async.forEach([ addresses ], function (address) {
            _gather.call(this, +address, lengths[address], async())
        })
    }, function () {
        // TODO permeate('bigeasy.strata.plan', {}, async())
        this.sheaf.tracer('plan', {}, async())
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
            if (difference > 0 && node.length > this.sheaf.options.leafSize) {
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
                if (node.length + node.right.length > this.sheaf.options.leafSize) {
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
                    parameters: [
                        node.right.key, node.key, lengths, !! this.ghosts[node.address]
                    ]
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
            async.forEach([ operations ], function (operation) {
                this[operation.method].apply(this, operation.parameters.concat(async()))
            })
        }, function () {
            this.sheaf.balancing = false
            return false
        })
    })
})

Balancer.prototype.shouldSplitBranch = function (branch, key, callback) {
    if (branch.items.length > this.sheaf.options.branchSize) {
        if (branch.address == 0) {
            this.drainRoot(callback)
        } else {
            this.splitBranch(branch.address, key, callback)
        }
    } else {
        callback(null)
    }
}

Balancer.prototype.splitLeafAndUnlock = cadence(function (async, address, key, ghosts) {
    var locker = this.sheaf.createLocker(),
        script = this.logger.createScript(),
        descents = [], replacements = [], encached = [],
        completed = 0,
        penultimate, leaf, split, pages, page,
        records, remainder, right, index, offset, length

    async.loop([], [function () {
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
        descents.push(penultimate = new Descent(this.sheaf, locker))

        penultimate.descend(address == 1 ? penultimate.left : penultimate.key(key),
                            penultimate.penultimate, async())
    }, function () {
        penultimate.upgrade(async())
    }, function () {
        descents.push(leaf = penultimate.fork())
        leaf.descend(address == 1 ? leaf.left : leaf.key(key), leaf.leaf, async())
    }, function () {
        split = leaf.page
        if (split.items.length - split.ghosts <= this.sheaf.options.leafSize) {
            this.sheaf.unbalanced(split, true)
            return [ async.break, false ]
        }
    }, function () {
        pages = Math.ceil(split.items.length / this.sheaf.options.leafSize)
        records = Math.floor(split.items.length / pages)
        remainder = split.items.length % pages

        right = split.right

        offset = split.items.length

        var splits = 0
        async.loop([], function () {
            if (splits++ == pages - 1) return [ async.break ]
            page = locker.encache(this.sheaf.createPage(1))
            encached.push(page)

            page.right = right

            length = remainder-- > 0 ? records + 1 : records
            offset = split.items.length - length
            index = offset

            penultimate.page.splice(penultimate.index + 1, 0, {
                key: split.items[offset].key,
                heft: 0,
                address: page.address
            })

            for (var i = 0; i < length; i++) {
                var item = split.items[index]

                ok(index < split.items.length)

                page.splice(page.items.length, 0, item)

                index++
            }

            right = {
                address: page.address,
                key: page.items[0].key
            }
        }, function () {
            split.splice(offset, length)
            script.rewriteLeaf(page)
        })
    }, function () {
        split.right = right
        script.rewriteLeaf(split)
        script.writeBranch(penultimate.page)
        script.commit(async())
    }, function () {
        this.sheaf.unbalanced(leaf.page, true)
        this.sheaf.unbalanced(page, true)
        return [ async.break, true, penultimate.page, encached[0].items[0].key ]
    })
})

Balancer.prototype.splitLeaf = cadence(function (async, address, key, ghosts) {
    async(function () {
        this.splitLeafAndUnlock(address, key, ghosts, async())
    }, function (split, penultimate, partition) {
        if (split) {
            this.shouldSplitBranch(penultimate, partition, async())
        }
    })
})

Balancer.prototype.splitBranchAndUnlock = cadence(function (async, address, key) {
    var locker = this.sheaf.createLocker(),
        script = this.logger.createScript(),
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
        descents.push(parent = new Descent(this.sheaf, locker))
        parent.descend(parent.key(key), parent.child(address), async())
    }, function () {
        parent.upgrade(async())
    }, function () {
        descents.push(full = parent.fork())
        full.descend(full.key(key), full.level(full.depth + 1), async())
    }, function () {
        split = full.page

        pages = Math.ceil(split.items.length / this.sheaf.options.branchSize)
        records = Math.floor(split.items.length / pages)
        remainder = split.items.length % pages

        offset = split.items.length

        for (var i = 0; i < pages - 1; i++ ) {
            var page = locker.encache(this.sheaf.createPage(0))

            children.push(page)
            encached.push(page)

            var length = remainder-- > 0 ? records + 1 : records
            var offset = split.items.length - length

            var cut = split.splice(offset, length)

            parent.page.splice(parent.index + 1, 0, {
                key: cut[0].key,
                address: page.address,
                heft: cut[0].heft
            })

            delete cut[0].key
            cut[0].heft = 0

            page.splice(0, 0, cut)
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
})

Balancer.prototype.splitBranch = cadence(function (async, address, key) {
    async(function () {
        this.splitBranchAndUnlock(address, key, async())
    }, function (parent, key) {
        this.shouldSplitBranch(parent, key, async())
    })
})

Balancer.prototype.drainRootAndUnlock = cadence(function (async, sheaf) {
    var locker = this.sheaf.createLocker(),
        script = this.logger.createScript(),
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
        pages = Math.ceil(root.items.length / this.sheaf.options.branchSize)
        records = Math.floor(root.items.length / pages)
        remainder = root.items.length % pages
        var lift = []

        for (var i = 0; i < pages; i++) {
            var page = locker.encache(this.sheaf.createPage(0))

            var length = remainder-- > 0 ? records + 1 : records
            var offset = root.items.length - length

            var cut = root.splice(offset, length)

            lift.push({
                key: cut[0].key,
                address: page.address,
                heft: cut[0].heft
            })
            children.push(page)

            delete cut[0].key
            cut[0].heft = 0

            page.splice(0, 0, cut)
        }

        lift.reverse()

        root.splice(0, 0, lift)

        children.forEach(function (page) {
            script.writeBranch(page)
        })
        script.writeBranch(root)
        script.commit(async())
    }, function () {
        return [ root ]
    })
})

Balancer.prototype.drainRoot = cadence(function (async, sheaf) {
    async(function () {
        this.drainRootAndUnlock(async())
    }, function (root) {
        if (root.items.length > this.sheaf.options.branchSize) this.drainRoot(async())
    })
})

Balancer.prototype.exorcise = function (pivot, page, corporal) {
    var entry

    ok(page.ghosts, 'no ghosts')
    ok(corporal.items.length - corporal.ghosts > 0, 'no replacement')

    // TODO how is this not a race condition? I'm writing to the log, but I've
    // not updated the pivot page, not rewritten during `deleteGhosts`.
    page.splice(0, 1, corporal.splice(corporal.ghosts, 1))
    page.ghosts = 0

    var item = pivot.page.splice(pivot.index, 1).shift()
    item.key = page.items[0].key
    item.heft = page.items[0].heft
    pivot.page.splice(pivot.index, 0, item)
}

Balancer.prototype.deleteGhost = cadence(function (async, key) {
    var locker = this.sheaf.createLocker(),
        script = this.logger.createScript(),
        descents = [],
        pivot, leaf, reference
    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(pivot = new Descent(this.sheaf, locker))
        pivot.descend(pivot.key(key), pivot.found([key]), async())
    }, function () {
        pivot.upgrade(async())
    }, function () {
        if (pivot.index != 0) {
            descents.push(reference = pivot.fork())
            reference.setIndex(reference.index - 1)
            reference.descend(reference.right, reference.leaf, async())
        }
    }, function () {
        descents.push(leaf = pivot.fork())
        leaf.descend(leaf.key(key), leaf.leaf, async())
    }, function () {
        this.exorcise(pivot, leaf.page, leaf.page)
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
})

Balancer.prototype.referring = cadence(function (async, leftKey, descents, pivot, pages) {
    var referring
    if (leftKey != null && pages.referring == null) {
        descents.push(referring = pages.referring = pivot.fork())
        async(function () {
            var key = referring.page.items[referring.index].key
            if (this.sheaf.comparator(leftKey, key) !== 0) {
                referring.setIndex(referring.index - 1)
                referring.descend(referring.key(leftKey), referring.found([leftKey]), async())
            }
        }, function () {
            var key = referring.page.items[referring.index].key
            ok(this.sheaf.comparator(leftKey, key) === 0, 'cannot find left key')
            referring.setIndex(referring.index - 1)
            referring.descend(referring.right, referring.leaf, async())
        })
    }
})

Balancer.prototype.mergePagesAndUnlock = cadence(function (
    async, key, leftKey, stopper, merger, ghostly
) {
    var locker = this.sheaf.createLocker(),
        script = this.logger.createScript(),
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

    async.loop([], [function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        ! [ 'left', 'right' ].forEach(function (direction) {
            // TODO use `pages` array, these conditions are tricky,
            // `parents[direction]` may not yet exist.
            if (singles[direction].length) {
                singles[direction].forEach(function (page) { locker.unlock(page) })
            } else if (parents[direction]) {
                locker.unlock(parents[direction].page)
            }
        })
        locker.dispose()
    }], function () {
        descents.push(pivot = new Descent(this.sheaf, locker))
        pivot.descend(pivot.key(key), pivot.found(keys), async())
    }, function () {
        var found = pivot.page.items[pivot.index].key
        if (this.sheaf.comparator(found, keys[0]) == 0) {
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
        parents.left.setIndex(parents.left.index - 1)
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
        if (!dirty) return [ async.break, false ]
    }, function () {
        var index = parents.right.indexes[ancestor.address]

        designation = ancestor.splice(index, 1).shift()

        if (pivot.page.address != ancestor.address) {
            ok(!index, 'expected ancestor to be removed from zero index')
            ok(ancestor.items[index], 'expected ancestor to have right sibling')
            designation = ancestor.splice(index, 1).shift()
            var hoist = pivot.page.splice(pivot.index, 1).shift()
            pivot.page.splice(pivot.index, 0, {
                key: designation.key,
                address: hoist.address,
                heft: designation.heft
            })
            ancestor.splice(index, 0, { address: designation.address, heft: 0 })
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
        return [ async.break, true, ancestor, designation.key ]
    })
})

Balancer.prototype.mergePages = cadence(function (async, key, leftKey, stopper, merger, ghostly) {
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
})

Balancer.prototype.mergeLeaves = function (key, leftKey, unbalanced, ghostly, callback) {
    function stopper (descent) { return descent.penultimate }

    var merger = cadence(function (async, script, leaves, ghosted) {
        ok(leftKey == null ||
           this.sheaf.comparator(leftKey, leaves.left.page.items[0].key) == 0,
           'left key is not as expected')
        ok(leftKey == null || leaves.referring != null, 'no referring page')
        ok(leftKey != null || leaves.referring == null, 'referring page when leftmost')

        var left = (leaves.left.page.items.length - leaves.left.page.ghosts)
        var right = (leaves.right.page.items.length - leaves.right.page.ghosts)

        this.sheaf.unbalanced(leaves.left.page, true)

        var index, referrantDirty
        if (left + right > this.sheaf.options.leafSize) {
            if (unbalanced[leaves.left.page.address]) {
                this.sheaf.unbalanced(leaves.left.page, true)
            }
            if (unbalanced[leaves.right.page.address]) {
                this.sheaf.unbalanced(leaves.right.page, true)
            }
            return [ false ]
        } else {
            async(function () {
                if (ghostly && left + right) {
                    referrantDirty = true
                    if (left) {
                        this.exorcise(ghosted, leaves.left.page, leaves.left.page)
                    } else {
                        this.exorcise(ghosted, leaves.left.page, leaves.right.page)
                    }
                }
            }, function () {
                leaves.left.page.right = leaves.right.page.right
                var ghosts = leaves.right.page.ghosts
                var count = leaves.right.page.items.length - leaves.right.page.ghosts
                var index = 0
                async.loop([], function () {
                    if (index == count) return [ async.break ]
                    var item = leaves.right.page.items[index + ghosts]
                    leaves.left.page.splice(leaves.left.page.items.length, 0, item)
                    index++
                })
            }, function () {
                leaves.right.page.splice(0, leaves.right.page.items.length)
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
}

Balancer.prototype.chooseBranchesToMergeAndUnlock = cadence(function (async, key, address) {
    var locker = this.sheaf.createLocker(),
        descents = [],
        designator, choice, lesser, greater, center

    var goToPage = cadence(function (async, descent, address, direction) {
        async(function () {
            descents.push(descent)
            descent.descend(descent.key(key), descent.address(address), async())
        }, function () {
            descent.setIndex(descent.index + (direction == 'left' ? 1 : -1))
                                        // ^^^ This ain't broke.
            descent.descend(descent[direction], descent.level(center.depth), async())
        })
    })

    async.loop([], function () {
        async([function () {
            descents.forEach(function (descent) { locker.unlock(descent.page) })
            locker.dispose()
        }], function () {
            descents.push(center = new Descent(this.sheaf, locker))
            center.descend(center.key(key), center.address(address), async())
        }, function () {
            if (center.lesser != null) {
                goToPage(lesser = new Descent(this.sheaf, locker), center.lesser, 'right', async())
            }
        }, function () {
            if (center.greater != null) {
                goToPage(greater = new Descent(this.sheaf, locker), center.greater, 'left', async())
            }
        }, function () {
            if (lesser && lesser.page.items.length + center.page.items.length <=
            this.sheaf.options.branchSize) {
                choice = center
            } else if (greater && greater.page.items.length + center.page.items.length <= this.sheaf.options.branchSize) {
                choice = greater
            }

            if (choice) {
                descents.push(designator = choice.fork())
                designator.setIndex(0)
                designator.descend(designator.left, designator.leaf, async())
            } else {
                return [ async.break, false ]
            }
        }, function () {
            var item = designator.page.items[0]
            return [ async.break, true, item.key, item.heft, choice.page.address ]
        })
    })
})

Balancer.prototype.chooseBranchesToMerge = cadence(function (async, key, address) {
    async(function () {
        this.chooseBranchesToMergeAndUnlock(key, address, async())
    }, function (merge, key, heft, address) {
        if (merge) {
            this.mergeBranches(key, heft, address, async())
        }
    })
})

Balancer.prototype.mergeBranches = function (key, heft, address, callback) {
    function stopper (descent) {
        return descent.child(address)
    }

    var merger = cadence(function (async, script, pages, ghosted) {
        ok(address == pages.right.page.address, 'unexpected address')

        var cut = pages.right.page.splice(0, pages.right.page.items.length)

        cut[0].key = key
        cut[0].heft = heft

        pages.left.page.splice(pages.left.page.items.length, 0, cut)

        script.writeBranch(pages.left.page)

        return true
    })

    this.mergePages(key, null, stopper, merger, false, callback)
}

Balancer.prototype.fillRoot = cadence(function (async, sheaf) {
    var locker = this.sheaf.createLocker(), script = this.logger.createScript(),
        descents = [], root, child

    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(root = new Descent(this.sheaf, locker))
        root.exclude()
        root.descend(root.left, root.level(0), async())
    }, function () {
        descents.push(child = root.fork())
        child.descend(child.left, child.level(1), async())
    }, function () {
        var cut
        ok(root.page.items.length == 1, 'only one address expected')

        root.page.splice(0, root.page.items.length)

        cut = child.page.splice(0, child.page.items.length)

        root.page.splice(root.page.items.length, 0, cut)

        script.writeBranch(root.page)
        script.unlink(child.page)
        script.commit(async())
    })
})

module.exports = Balancer
