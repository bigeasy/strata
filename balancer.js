var ok = require('assert').ok
var cadence = require('cadence/redux')
var Script = require('./script')
var Descent = require('./descent')

function Balancer () {
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

Balancer.prototype._nodify = cadence(function (async, sheaf, locker, page) {
    async(function () {
        this._node(locker, page, async())
    }, function (node) {
        async(function () {
            sheaf.tracer('reference', {}, async())
        }, function () {
            return node
        })
    })
})

// to user land
Balancer.prototype.balance = cadence(function balance (async, sheaf) {
    var locker = sheaf.createLocker(), operations = [], address, length

    var _gather = cadence(function (async, address, length) {
        var right, node
        async(function () {
            if (node = this.ordered[address]) {
                return [ node ]
            } else {
                async(function () {
                    locker.lock(address, false, async())
                }, function (page) {
                    this._nodify(sheaf, locker, page, async())
                })
            }
        }, function (node) {
            if (!(node.length - length < 0)) return
            if (node.address != 1 && ! node.left) async(function () {
                var descent = new Descent(sheaf, locker)
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
                        this._nodify(sheaf, locker, descent.page, async())
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
                    this._nodify(sheaf, locker, page, async())
                })
            }, function (right) {
                node.right = right
                right.left = node
            })
        })
    })

    ok(!sheaf.balancing, 'already balancing')

    var lengths = sheaf.lengths, addresses = Object.keys(lengths)
    if (addresses.length == 0) {
        return [ async, true ]
    } else {
        sheaf.lengths = {}
        this.operations = []
        this.ordered = {}
        this.ghosts = {}
        sheaf.balancing = true
    }

    async(function () {
        async.forEach(function (address) {
            _gather.call(this, +address, lengths[address], async())
        })(addresses)
    }, function () {
        sheaf.tracer('plan', {}, async())
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
            if (difference > 0 && node.length > sheaf.options.leafSize) {
                operations.unshift({
                    method: 'splitLeaf',
                    parameters: [ sheaf, node.address, node.key, this.ghosts[node.address] ]
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
                if (node.length + node.right.length > sheaf.options.leafSize) {
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
                        sheaf, node.right.key, node.key, lengths, !! this.ghosts[node.address]
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
                parameters: [ sheaf, node.key ]
            })
        }

        async(function () {
            async.forEach(function (operation) {
                this[operation.method].apply(this, operation.parameters.concat(async()))
            })(operations)
        }, function () {
            sheaf.balancing = false
            return false
        })
    })
})

Balancer.prototype.shouldSplitBranch = function (sheaf, branch, key, callback) {
    if (branch.items.length > sheaf.options.branchSize) {
        if (branch.address == 0) {
            this.drainRoot(sheaf, callback)
        } else {
            this.splitBranch(sheaf, branch.address, key, callback)
        }
    } else {
        callback(null)
    }
}

Balancer.prototype.splitLeafAndUnlock = cadence(function (async, sheaf, address, key, ghosts) {
    var locker = sheaf.createLocker(),
        script = new Script(sheaf),
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
            this.deleteGhost(sheaf, key, async())
        }, function (rekey) {
            key = rekey
        })
    }, function () {
        descents.push(penultimate = new Descent(sheaf, locker))

        penultimate.descend(address == 1 ? penultimate.left : penultimate.key(key),
                            penultimate.penultimate, async())
    }, function () {
        penultimate.upgrade(async())
    }, function () {
        descents.push(leaf = penultimate.fork())
        leaf.descend(address == 1 ? leaf.left : leaf.key(key), leaf.leaf, async())
    }, function () {
        split = leaf.page
        if (split.items.length - split.ghosts <= sheaf.options.leafSize) {
            sheaf.unbalanced(split, true)
            return [ splitter, false ]
        }
    }, function () {
        pages = Math.ceil(split.items.length / sheaf.options.leafSize)
        records = Math.floor(split.items.length / pages)
        remainder = split.items.length % pages

        right = split.right

        offset = split.items.length

        var splits = 0
        var loop = async(function () {
            if (splits++ == pages - 1) return [ loop ]
            page = locker.encache(sheaf.createLeaf({ loaded: true }))
            encached.push(page)

            page.right = right

            length = remainder-- > 0 ? records + 1 : records
            offset = split.items.length - length
            index = offset

            sheaf.splice(penultimate.page, penultimate.index + 1, 0, {
                key: split.items[offset].key,
                heft: sheaf.serialize(split.items[offset].key, true).length,
                address: page.address
            })

            for (var i = 0; i < length; i++) {
                var item = split.items[index]

                ok(index < split.items.length)

                sheaf.splice(page, page.items.length, 0, item)

                index++
            }

            right = {
                address: page.address,
                key: page.items[0].key
            }
        }, function () {
            sheaf.splice(split, offset, length)
            script.rewriteLeaf(page)
        })()
    }, function () {
        split.right = right
        script.rewriteLeaf(split)
        script.writeBranch(penultimate.page)
        script.commit(async())
    }, function () {
        sheaf.unbalanced(leaf.page, true)
        sheaf.unbalanced(page, true)
        return [ splitter, true, penultimate.page, encached[0].items[0].key ]
    })()
})

Balancer.prototype.splitLeaf = cadence(function (async, sheaf, address, key, ghosts) {
    async(function () {
        this.splitLeafAndUnlock(sheaf, address, key, ghosts, async())
    }, function (split, penultimate, partition) {
        if (split) {
            this.shouldSplitBranch(sheaf, penultimate, partition, async())
        }
    })
})

Balancer.prototype.splitBranchAndUnlock = cadence(function (async, sheaf, address, key) {
    var locker = sheaf.createLocker(),
        script = new Script(sheaf),
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
        descents.push(parent = new Descent(sheaf, locker))
        parent.descend(parent.key(key), parent.child(address), async())
    }, function () {
        parent.upgrade(async())
    }, function () {
        descents.push(full = parent.fork())
        full.descend(full.key(key), full.level(full.depth + 1), async())
    }, function () {
        split = full.page

        pages = Math.ceil(split.items.length / sheaf.options.branchSize)
        records = Math.floor(split.items.length / pages)
        remainder = split.items.length % pages

        offset = split.items.length

        for (var i = 0; i < pages - 1; i++ ) {
            var page = locker.encache(sheaf.createBranch({}))

            children.push(page)
            encached.push(page)

            var length = remainder-- > 0 ? records + 1 : records
            var offset = split.items.length - length

            var cut = sheaf.splice(split, offset, length)

            sheaf.splice(parent.page, parent.index + 1, 0, {
                key: cut[0].key,
                address: page.address,
                heft: cut[0].heft
            })

            delete cut[0].key
            cut[0].heft = 0

            sheaf.splice(page, 0, 0, cut)
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

Balancer.prototype.splitBranch = cadence(function (async, sheaf, address, key) {
    async(function () {
        this.splitBranchAndUnlock(sheaf, address, key, async())
    }, function (parent, key) {
        this.shouldSplitBranch(sheaf, parent, key, async())
    })
})

Balancer.prototype.drainRootAndUnlock = cadence(function (async, sheaf) {
    var locker = sheaf.createLocker(),
        script = new Script(sheaf),
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
        pages = Math.ceil(root.items.length / sheaf.options.branchSize)
        records = Math.floor(root.items.length / pages)
        remainder = root.items.length % pages
        var lift = []

        for (var i = 0; i < pages; i++) {
            var page = locker.encache(sheaf.createBranch({}))

            var length = remainder-- > 0 ? records + 1 : records
            var offset = root.items.length - length

            var cut = sheaf.splice(root, offset, length)

            lift.push({
                key: cut[0].key,
                address: page.address,
                heft: cut[0].heft
            })
            children.push(page)

            delete cut[0].key
            cut[0].heft = 0

            sheaf.splice(page, 0, 0, cut)
        }

        lift.reverse()

        sheaf.splice(root, 0, 0, lift)

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
        this.drainRootAndUnlock(sheaf, async())
    }, function (root) {
        if (root.items.length > sheaf.options.branchSize) this.drainRoot(sheaf, async())
    })
})

Balancer.prototype.exorcise = function (sheaf, pivot, page, corporal) {
    var entry

    ok(page.ghosts, 'no ghosts')
    ok(corporal.items.length - corporal.ghosts > 0, 'no replacement')

    // todo: how is this not a race condition? I'm writing to the log, but I've
    // not updated the pivot page, not rewritten during `deleteGhosts`.
    sheaf.splice(page, 0, 1, sheaf.splice(corporal, corporal.ghosts, 1))
    page.ghosts = 0

    var item = sheaf.splice(pivot.page, pivot.index, 1).shift()
    item.key = page.items[0].key
    item.heft = page.items[0].heft
    sheaf.splice(pivot.page, pivot.index, 0, item)
}

Balancer.prototype.deleteGhost = cadence(function (async, sheaf, key) {
    var locker = sheaf.createLocker(),
        script = new Script(sheaf),
        descents = [],
        pivot, leaf, reference
    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(pivot = new Descent(sheaf, locker))
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
        this.exorcise(sheaf, pivot, leaf.page, leaf.page)
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

Balancer.prototype.referring = cadence(function (async, sheaf, leftKey, descents, pivot, pages) {
    var referring
    if (leftKey != null && pages.referring == null) {
        descents.push(referring = pages.referring = pivot.fork())
        async(function () {
            var key = referring.page.items[referring.index].key
            if (sheaf.comparator(leftKey, key) !== 0) {
                referring.index--
                referring.descend(referring.key(leftKey), referring.found([leftKey]), async())
            }
        }, function () {
            var key = referring.page.items[referring.index].key
            ok(sheaf.comparator(leftKey, key) === 0, 'cannot find left key')
            referring.index--
            referring.descend(referring.right, referring.leaf, async())
        })
    }
})

Balancer.prototype.mergePagesAndUnlock = cadence(function (
    async, sheaf, key, leftKey, stopper, merger, ghostly
) {
    var locker = sheaf.createLocker(),
        script = new Script(sheaf),
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
        descents.push(pivot = new Descent(sheaf, locker))
        pivot.descend(pivot.key(key), pivot.found(keys), async())
    }, function () {
        var found = pivot.page.items[pivot.index].key
        if (sheaf.comparator(found, keys[0]) == 0) {
            pivot.upgrade(async())
        } else {
            async(function () { // left above right
                pivot.upgrade(async())
            }, function () {
                this.referring(sheaf, leftKey, descents, pivot, pages, async())
            }, function () {
                ghosted = { page: pivot.page, index: pivot.index }
                descents.push(pivot = pivot.fork())
                keys.pop()
                pivot.descend(pivot.key(key), pivot.found(keys), async())
            })
        }
    }, function () {
        this.referring(sheaf, leftKey, descents, pivot, pages, async())
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

        designation = sheaf.splice(ancestor, index, 1).shift()

        if (pivot.page.address != ancestor.address) {
            ok(!index, 'expected ancestor to be removed from zero index')
            ok(ancestor.items[index], 'expected ancestor to have right sibling')
            designation = sheaf.splice(ancestor, index, 1).shift()
            var hoist = sheaf.splice(pivot.page, pivot.index, 1).shift()
            sheaf.splice(pivot.page, pivot.index, 0, {
                key: designation.key,
                address: hoist.address,
                heft: designation.heft
            })
            sheaf.splice(ancestor, index, 0, { address: designation.address, heft: 0 })
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
})

Balancer.prototype.mergePages = cadence(function (async, sheaf, key, leftKey, stopper, merger, ghostly) {
    async(function () {
        this.mergePagesAndUnlock(sheaf, key, leftKey, stopper, merger, ghostly, async())
    }, function (merged, ancestor, designation) {
        if (merged) {
            if (ancestor.address == 0) {
                if (ancestor.items.length == 1 && !(ancestor.items[0].address % 2)) {
                    this.fillRoot(sheaf, async())
                }
            } else {
                this.chooseBranchesToMerge(sheaf, designation, ancestor.address, async())
            }
        }
    })
})

Balancer.prototype.mergeLeaves = function (sheaf, key, leftKey, unbalanced, ghostly, callback) {
    function stopper (descent) { return descent.penultimate }

    var merger = cadence(function (async, script, leaves, ghosted) {
        ok(leftKey == null ||
           sheaf.comparator(leftKey, leaves.left.page.items[0].key) == 0,
           'left key is not as expected')
        ok(leftKey == null || leaves.referring != null, 'no referring page')
        ok(leftKey != null || leaves.referring == null, 'referring page when leftmost')

        var left = (leaves.left.page.items.length - leaves.left.page.ghosts)
        var right = (leaves.right.page.items.length - leaves.right.page.ghosts)

        sheaf.unbalanced(leaves.left.page, true)

        var index, referrantDirty
        if (left + right > sheaf.options.leafSize) {
            if (unbalanced[leaves.left.page.address]) {
                sheaf.unbalanced(leaves.left.page, true)
            }
            if (unbalanced[leaves.right.page.address]) {
                sheaf.unbalanced(leaves.right.page, true)
            }
            return [ false ]
        } else {
            async(function () {
                if (ghostly && left + right) {
                    referrantDirty = true
                    if (left) {
                        this.exorcise(sheaf, ghosted, leaves.left.page, leaves.left.page)
                    } else {
                        this.exorcise(sheaf, ghosted, leaves.left.page, leaves.right.page)
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
                    sheaf.splice(leaves.left.page, leaves.left.page.items.length, 0, item)
                    index++
                })()
            }, function () {
                sheaf.splice(leaves.right.page, 0, leaves.right.page.items.length)
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

    this.mergePages(sheaf, key, leftKey, stopper, merger, ghostly, callback)
}

Balancer.prototype.chooseBranchesToMergeAndUnlock = cadence(function (async, sheaf, key, address) {
    var locker = sheaf.createLocker(),
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
            descents.push(center = new Descent(sheaf, locker))
            center.descend(center.key(key), center.address(address), async())
        }, function () {
            if (center.lesser != null) {
                goToPage(lesser = new Descent(sheaf, locker), center.lesser, 'right', async())
            }
        }, function () {
            if (center.greater != null) {
                goToPage(greater = new Descent(sheaf, locker), center.greater, 'left', async())
            }
        }, function () {
            if (lesser && lesser.page.items.length + center.page.items.length <= sheaf.options.branchSize) {
                choice = center
            } else if (greater && greater.page.items.length + center.page.items.length <= sheaf.options.branchSize) {
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
})

Balancer.prototype.chooseBranchesToMerge = cadence(function (async, sheaf, key, address) {
    async(function () {
        this.chooseBranchesToMergeAndUnlock(sheaf, key, address, async())
    }, function (merge, key, heft, address) {
        if (merge) {
            this.mergeBranches(sheaf, key, heft, address, async())
        }
    })
})

Balancer.prototype.mergeBranches = function (sheaf, key, heft, address, callback) {
    function stopper (descent) {
        return descent.child(address)
    }

    var merger = cadence(function (async, script, pages, ghosted) {
        ok(address == pages.right.page.address, 'unexpected address')

        var cut = sheaf.splice(pages.right.page, 0, pages.right.page.items.length)

        cut[0].key = key
        cut[0].heft = heft

        sheaf.splice(pages.left.page, pages.left.page.items.length, 0, cut)

        script.writeBranch(pages.left.page)

        return true
    })

    this.mergePages(sheaf, key, null, stopper, merger, false, callback)
}

Balancer.prototype.fillRoot = cadence(function (async, sheaf) {
    var locker = sheaf.createLocker(), script = new Script(sheaf), descents = [], root, child

    async([function () {
        descents.forEach(function (descent) { locker.unlock(descent.page) })
        locker.dispose()
    }], function () {
        descents.push(root = new Descent(sheaf, locker))
        root.exclude()
        root.descend(root.left, root.level(0), async())
    }, function () {
        descents.push(child = root.fork())
        child.descend(child.left, child.level(1), async())
    }, function () {
        var cut
        ok(root.page.items.length == 1, 'only one address expected')

        sheaf.splice(root.page, 0, root.page.items.length)

        cut = sheaf.splice(child.page, 0, child.page.items.length)

        sheaf.splice(root.page, root.page.items.length, 0, cut)

        script.writeBranch(root.page)
        script.unlink(child.page)
        script.commit(async())
    })
})

module.exports = Balancer
