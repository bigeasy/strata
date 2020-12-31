'use strict'

// Sort function generator.
const ascension = require('ascension')
// Comparator decorator that extracts the sorted fields from an object.
const whittle = require('whittle')

// Node.js API.
const assert = require('assert')
const path = require('path')
const fileSystem = require('fs')
const fs = require('fs').promises

// Sensible `async`/`await` over Node.js streams.
const Staccato = require('staccato/redux')

// Return the first non null-like value.
const coalesce = require('extant')

// Wraps a `Promise` in an object to act as a mutex.
const Future = require('prospective/future')

// An `async`/`await` work queue.
const Turnstile = require('turnstile')

// Journaled file system operations for tree rebalancing.
const Journalist = require('journalist')

// A pausable service work queue that shares a common application work queue.
const Fracture = require('fracture')

// A non-crypographic (fast) 32-bit hash for record integrity.
// const fnv = require('./fnv')

// Serialize a single b-tree record.
const Recorder = require('transcript/recorder')

// Incrementally read a b-tree page chunk by chunk.
const Player = require('transcript/player')

// Binary search for a record in a b-tree page.
const find = require('./find')

const Partition = require('./partition')

const io = require('./io')

const rescue = require('rescue')

// Currently unused.
function traceIf (condition) {
    if (condition) return function (...vargs) {
        console.log.apply(console, vargs)
    }
    return function () {}
}

// An `Error` type specific to Strata.
const Strata = { Error: require('./error') }

// A latch.
function latch () {
    let capture
    return { unlocked: false, promise: new Promise(resolve => capture = { resolve }), ...capture }
}

// Sheaf is the crux of Strata. It exists as a separate object possibly for
// legacy reasons, and it will stay that way because it makes `Strata` and
// `Cursor` something a user can read to understand the interface.

//
class Sheaf {
    // Used to identify the pages of this instance in the page cache which can
    // be shared across different Strata. We do not want to pull pages from the
    // cache based only on the directory path and page id because we may close
    // and reopen a Strata and we'd pull pages from the previous instance.
    static _instance = 0
    static __instance = 0

    static options (options) {
        if (options.checksum == null) {
            options.checksum = (() => '0')
        }
        if (options.extractor == null) {
            options.extractor = parts => parts[0]
        }
        options.serializer = function () {
            const serializer = coalesce(options.serializer, 'json')
            switch (serializer) {
            case 'json':
                return {
                    parts: {
                        serialize: function (parts) {
                            return parts.map(part => Buffer.from(JSON.stringify(part)))
                        },
                        deserialize: function (parts) {
                            return parts.map(part => JSON.parse(part.toString()))
                        }
                    },
                    key: {
                        serialize: function (key) {
                            if (key == null) {
                                throw new Error
                            }
                            return [ Buffer.from(JSON.stringify(key)) ]
                        },
                        deserialize: function (parts) {
                            return JSON.parse(parts[0].toString())
                        }
                    }
                }
            case 'buffer':
                return {
                    parts: {
                        serialize: function (parts) { return parts },
                        deserialize: function (parts) { return parts }
                    },
                    key: {
                        serialize: function (part) { return [ part ] },
                        deserialize: function (parts) { return parts[0] }
                    }
                }
            default:
                return serializer
            }
        } ()
        const leaf = coalesce(options.leaf, {})
        options.leaf = {
            split: coalesce(leaf.split, 5),
            merge: coalesce(leaf.merge, 1)
        }
        const branch = coalesce(options.branch, {})
        options.branch = {
            split: coalesce(branch.split, 5),
            merge: coalesce(branch.merge, 1)
        }
        options.comparator = function () {
            const zero = object => object
            if (options.comparator == null) {
                const comparator = whittle(ascension([ String ]), value => [ value ])
                return { leaf: comparator, branch: comparator, zero }
            } else if (typeof options.comparator == 'function') {
                return { leaf: options.comparator, branch: options.comparator, zero }
            } else {
                return options.comparator
            }
        } ()
        return options
    }

    // Sheaf accepts the destructible and user options passed to `new Strata`
    constructor (destructible, options) {
        Strata.Error.assert(options.turnstile != null, 'OPTION_REQUIRED', { _option: 'turnstile' })
        assert(destructible.isDestroyedIfDestroyed(options.turnstile.destructible))

        this.options = Sheaf.options(options)

        this.pages = options.pages
        this.directory = options.directory
        this.checksum = options.checksum
        this.serializer = options.serializer
        this.extractor = options.extractor
        this.comparator = options.comparator
        this.serializer = options.serializer
        this.leaf = options.leaf
        this.branch = options.branch

        this.storage = options.storage.create(this)

        this._recorder = Recorder.create(() => '0')
        this._root = null

        this._id = 0
        this._destructible = destructible

        // **TODO** Do not worry about wrapping anymore.
        // Operation id wraps at 32-bits, cursors should not be open that long.
        this._operationId = 0xffffffff
        // Concurrency and work queues. One keyed queue for page writes, the
        // other queue will only use a single key for all housekeeping.
        this._fracture = {
            appender: new Fracture(destructible.durable($ => $(), 'appender'), options.turnstile, id => ({
                id: this._operationId = (this._operationId + 1 & 0xffffffff) >>> 0,
                writes: [],
                cartridge: this.pages.hold(id),
                latch: latch()
            }), this._append, this),
            housekeeper: new Fracture(destructible.durable($ => $(), 'housekeeper'), options.turnstile, () => ({
                candidates: []
            }), this._keephouse, this)
        }

        options.turnstile.deferrable.increment()

        this.closed = false
        this.destroyed = false


        // **TODO** Not yet used, would `mkdir` any pages that need to be
        // inspected for balance.
        this._canceled = new Set

        destructible.destruct(() => {
            this.destroyed = true
            destructible.ephemeral('shutdown', async () => {
                // Trying to figure out how to wait for the Turnstile to drain.
                // We can't terminate the housekeeping turnstile then the
                // acceptor turnstile because they depend on each other, so
                // we're going to loop. We wait for one to drain, then the
                // other, then check to see if anything is in the queues to
                // determine if we can leave the loop. Actually, we only need to
                // check the size of the first queue in the loop, the second
                // will be empty when `drain` returns.
                //
                // **TODO** Really want to just push keys into a file for
                // inspection when we reopen for housekeeping.
                await this.drain()
                options.turnstile.deferrable.decrement()
                if (this._root != null) {
                    this._root.cartridge.remove()
                    this._root = null
                }
            })
        })
    }

    create (strata) {
        this._root = this._create({ id: -1, leaf: false, items: [{ id: '0.0' }] }, [])
        return this._destructible.exceptional('create', async () => {
            await this.storage.create()
            return strata
        })
    }

    open (strata) {
        this._root = this._create({ id: -1, leaf: false, items: [{ id: '0.0' }] }, [])
        return this._destructible.exceptional('open', async () => {
            await this.storage.open()
            return strata
        })
    }

    read (id) {
        return this.storage.read(id)
    }

    // We load the page then check for a race after we've loaded. If a different
    // strand beat us to it, we just ignore the result of our read and return
    // the cached entry.

    //
    async load (id) {
        const { page, heft } = await this.read(id)
        const entry = this.pages.hold(id, null)
        if (entry.value == null) {
            entry.value = page
            entry.heft = heft
        }
        return entry
    }

    _create (page, cartridges) {
        const cartridge = this.pages.hold(page.id, page)
        cartridges.push(cartridge)
        return { page: cartridge.value, cartridge }
    }

    // TODO If `key` is `null` then just go left.
    _descend (entries, { key, level = -1, fork = false, rightward = false, approximate = false }) {
        const descent = { miss: null, keyed: null, level: 0, index: 0, entry: null,
            pivot: null,
            cartridge: null,
            page: null
        }
        let entry = null
        entries.push(entry = this.pages.hold(-1))
        for (;;) {
            // When you go rightward at the outset or fork you might hit this
            // twice, but it won't matter because you're not going to use the
            // pivot anyway.
            //
            // You'll struggle to remember this, but it is true...

            //
            if (descent.index != 0) {
                //

                // The last key we visit is the key for the leaf page for whatever
                // level we stop at. This holds true even if we fork. We hold onto
                // the previous pivot and if the left fork is not not the zero index
                // of the branch page, then the previous pivot is the key for the
                // leaf of the fork. Note that for balancing, we only fork when we
                // match the exact key in a brach. We have an approximate fork for
                // the user in case we eliminate the leaf page with a merge, they
                // will land in the merged page at the first index less than the
                // key. The right key tracking will also be correct since we will
                // immediately pick up a right key when we leave this block.

                //
                const pivot = descent.pivot
                descent.pivot = {
                    key: entry.value.items[descent.index].key,
                    level: descent.level - 1
                }
                //

                // If we're trying to find siblings we're using an exact key
                // that is definately above the level sought, we'll see it and
                // then go left or right if there is a branch in that direction.
                //
                // Earlier I had this at KILLROY below. And I adjust the level, but
                // I don't reference the level, so it's probably fine here.

                //
                if (this.comparator.branch(descent.pivot.key, key) == 0 && fork) {
                    descent.index--
                    rightward = true
                    descent.pivot = descent.index != 0
                        ? { key: entry.value.items[descent.index].key, level: descent.level - 1 }
                        : pivot
                }
            }

            // You don't fork right. You can track the rightward key though.
            if (descent.index + 1 < entry.value.items.length) {
                descent.right = entry.value.items[descent.index + 1].key
            }

            // We exit at the leaf, so this will always be a branch page.
            const id = entry.value.items[descent.index].id

            // Attempt to hold the page from the cache, return the id of the
            // page if we have a cache miss.
            entry = this.pages.hold(id)
            if (entry == null) {
                return { miss: id }
            }
            entries.push(entry)

            // TODO Move this down below the leaf return and do not search if
            // we are searching for a leaf.

            // Binary search the page for the key, or just go right or left
            // directly if there is no key.
            const offset = entry.value.leaf ? 0 : 1
            const index = rightward
                ? entry.value.leaf ? ~(entry.value.items.length - 1) : entry.value.items.length - 1
                : key != null
                    ? find(this.comparator.leaf, entry.value.items, key, offset)
                    : entry.value.leaf ? ~0 : 0
            //

            // If the page is a leaf, assert that we're looking for a leaf and
            // return the leaf page.

            //
            if (entry.value.leaf) {
                descent.found = index >= 0
                descent.index = index < 0 ? ~index : index
                assert.equal(level, -1, 'could not find branch')
                break
            }
            //

            // If the index is less than zero we didn't find the exact key, so
            // we're looking at the bitwise not of the insertion point which is
            // right after the branch we're supposed to descend, so back it up
            // one.

            //
            descent.index = index < 0 ? ~index - 1 : index

            // We're trying to reach branch and we've hit the level.
            if (level == descent.level) {
                break
            }

            // KILLROY was here.

            descent.level++
        }
        //

        // **TODO** What happens when we merge a leaf page so that the key is
        // gone and then we delete all the values before the key? Essentially,
        // what is the effect of searching for a key that is not a leaf key
        // whose value is greater than the leaf key it lands on and less than
        // the least value in the page? We can test this without branch races.
        // If it is `-1` that's fine. You're not supposed to fork to find an
        // insert location. I believe `-1` is a stop for reverse iteration.
        // Write a test and come back and document this with more confidence.

        //
        if (fork && !rightward) {
            if (approximate) {
                descent.index--
                descent.found = false
            } else {
                return null
            }
        }
        return descent
    }
    //

    // We hold onto the entries array for the descent to prevent the unlikely
    // race condition where we cannot descend because we have to load a page,
    // but while we're loading a page another page in the descent unloads.
    //
    // Conceivably, this could continue indefinitely.

    //
    async descend (query, callerEntries, internal = true) {
        const entries = [[]]
        for (;;) {
            entries.push([])
            const descent = this._descend(entries[1], query)
            entries.shift().forEach(entry => entry.release())
            if (descent == null) {
                entries.shift().forEach((entry) => entry.release())
                return null
            }
            if (descent.miss == null) {
                callerEntries.push(descent.entry = entries[0].pop())
                entries.shift().forEach(entry => entry.release())
                descent.cartridge = descent.entry
                descent.page = descent.cartridge.value
                return descent
            }
            const load = this.load(descent.miss)
            const entry = internal
                ? await load
                : await this._destructible.exceptional('load', load)
            entries[0].push(entry)
        }
    }

    descend2 (trampoline, query, found) {
        const entries = []
        try {
            const descent = this._descend(entries, query)
            if (descent.miss) {
                trampoline.promised(async () => {
                    try {
                        entries.push(await this._destructible.exceptional('load', this.load(descent.miss)))
                        this.descend2(trampoline, query, found)
                    } finally {
                        entries.forEach(entry => entry.release())
                    }
                })
            } else {
                if (descent != null) {
                    descent.entry = entries.pop()
                }
                entries.forEach(entry => entry.release())
                found(descent)
            }
        } catch (error) {
            entries.forEach(entry => entry.release())
            throw error
        }
    }

    // Writes appear to be able to run with impunity. What was the logic there?
    // Something about the leaf being written to synchronously, but if it was
    // asynchronous, then it is on the user to assert that the page has not
    // changed.
    //
    // The block will wait on a promise release preventing any of the writes
    // from writing.
    //
    // Keep in mind that there is only one housekeeper, so that might factor
    // into the logic here.
    //
    // Can't see what's preventing writes from becoming stale. Do I ensure that
    // they are written before the split? Must be.

    //
    async _append ({ canceled, key, value: { writes, cartridge, latch } }) {
        try {
            this._destructible.progress()
            const page = cartridge.value
            if (
                (
                    page.items.length >= this.leaf.split &&
                    this.comparator.branch(page.items[0].key, page.items[page.items.length - 1].key) != 0
                )
                ||
                (
                    ! (page.id == '0.1' && page.right == null) &&
                    page.items.length <= this.leaf.merge
                )
            ) {
                this._fracture.housekeeper.enqueue('housekeeping').candidates.push(page.key || page.items[0].key)
            }
            await this.storage.writeLeaf(page, writes)
        } finally {
            cartridge.release()
            latch.unlocked = true
            latch.resolve.call(null)
        }
    }

    append (id, buffer, writes) {
        // **TODO** Optional boolean other than `destroyed`.
        this._destructible.operational()
        const append = this._fracture.appender.enqueue(id)
        append.writes.push(buffer)
        if (writes[append.id] == null) {
            writes[append.id] = append.latch
        }
    }

    async drain () {
        do {
            await this._fracture.housekeeper.drain()
            await this._fracture.appender.drain()
        } while (this._fracture.housekeeper.count != 0)
    }

    recordify (header, parts = []) {
        return this._recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
    }

    _unbalanced (page) {
        return page.leaf
            ? (
                page.items.length >= this.leaf.split &&
                this.comparator.branch(page.items[0].key, page.items[page.items.length - 1].key) != 0
            )
            ||
            (
                ! (page.id == '0.1' && page.right == null) &&
                page.items.length <= this.leaf.merge
            )
            : (
                page.items.length >= this.branch.split
            )
            ||
            (
                page.id == '0.0'
                    ? +page.items[0].id.split('.')[1] % 2 == 0 && page.items.length == 1
                    : page.items.length <= this.branch.merge
            )
    }

    _balanceIf (branch, messages, message) {
        if (this._unbalanced(branch.page)) {
            messages.push(message)
        }
    }

    // Assume there is nothing to block or worry about with the branch pages.
    // Can't recall at the moment, though. Descents are all synchronous.
    //
    // You've come back to this and it really bothers you that these slices are
    // performed twice, once in the journalist and once in the commit. You
    // probably want to let this go for now until you can see clearly how you
    // might go about eliminating this duplication. Perhaps the commit uses the
    // journalist to descend, lock, etc. just as the Cursor does. Or maybe the
    // Journalist is just a Sheaf of pages, which does perform the leaf write,
    // but defers to the Commit, now called a Journalist, to do the splits.
    //
    // It is not the case that the cached information is in some format that is
    // not ready for serialization. What do we get exactly? What we'll see at
    // first is that these two are calling each other a lot, so we're going to
    // probably want to move more logic back over to Commit, including leaf
    // splits. It will make us doubt that we could ever turn this easily into an
    // R*Tree but the better the architecture, the easier it will be to extract
    // components for reuse as modules, as opposed to making this into some sort
    // of pluggable framework.
    //
    // Maybe it just came to me. Why am I logging `drain`, `fill`, etc? The
    // commit should just expose `emplace` and the journalist can do the split
    // and generate the pages and then the Commit is just journaled file system
    // operations. It won't even update the heft, it will just return the new
    // heft and maybe it doesn't do the page reads either.
    //
    // We'd only be duplicating the splices, really.

    //
    async _drainRoot (messages, cartridges) {
        const root = await this.descend({ key: null, level: 0 }, cartridges)

        const partition = Math.floor(root.entry.value.items.length / 2)

        const left = this._create({
            id: this.storage.nextId(false),
            offset: 1,
            items: root.page.items.slice(0, partition),
            hash: null
        }, cartridges)

        const right = this._create({
            id: this.storage.nextId(false),
            offset: 1,
            items: root.page.items.slice(partition),
            hash: null
        }, cartridges)

        root.page.items = [{
            id: left.page.id,
            key: null,
            heft: left.page.items[0].heft
        }, {
            id: right.page.id,
            key: right.page.items[0].key,
            heft: left.page.items[0].heft
        }]
        right.page.items[0].key = null
        right.page.items[0].heft = left.page.items[0].heft

        messages.forEach(message => message.level++)

        this._balanceIf(left, messages, { method: 'balance', key: null, level: 1 })
        this._balanceIf(right, messages, { method: 'balance', key: root.page.items[1].key, level: 1 })

        await this.storage.writeDrainRoot({ left, right, root })
    }

    async balance (key, level, messages, cartridges) {
        const branch = await this.descend({ key, level }, cartridges)
        const leaves = +branch.page.items[0].id.split('.')[1] % 2 == 1
        if (branch.page.items.length >= this.branch.split) {
            if (branch.page.id == '0.0') {
                await this._drainRoot(messages, cartridges)
            } else {
                await this._splitBranch(key, level, messages, cartridges)
            }
        } else if (branch.page.items.length <= this.branch.merge) {
            if (branch.page.id != '0.0') {
                // TODO Have `_selectMerger` manage its own entries.
                const merger = await this._selectMerger(key, branch, cartridges)
                await this._mergeBranch(merger, messages, cartridges)
            } else if (! leaves && branch.page.items.length == 1) {
                await this._fillRoot(messages, cartridges)
            }
        }
    }

    async _splitBranch (key, level, messages, cartridges) {
        const left = await this.descend({ key, level }, cartridges)
        const parent = await this.descend({ key, level: level - 1 }, cartridges)

        const partition = Math.floor(left.page.items.length / 2)

        const right = this._create({
            id: this.storage.nextId(false),
            items: left.page.items.splice(partition),
            leaf: false
        }, cartridges)

        const promotion = right.page.items[0].key
        right.page.items[0].key = null
        left.page.items = left.page.items.splice(0, partition)
        parent.page.items.splice(parent.index + 1, 0, {
            key: promotion,
            id: right.page.id,
            heft: parent.page.items[parent.page.items.length - 1].heft
        })

        this._balanceIf(left, messages, { method: 'balance', key: key, level: level })
        this._balanceIf(right, messages, { method: 'balance', key: promotion, level: level })
        this._balanceIf(parent, messages, { method: 'balance', key: key, level: level - 1 })

        await this.storage.writeSplitBranch({ promotion, left, right, parent })
    }
    //

    // **TODO** This is what we'll call a vacuum for the sake of removing delete
    // messages.

    //
    async _rotate () {
    }
    //

    // Split leaf. We always split a new page off to the right. Because we
    // always merge two pages together into the left page our left-most page id
    // will never change, it will always be `0.1`.
    //
    // Split is performed by creating two new stub append log. One for the
    // existing page which is now the left page and one for the new right page.
    // When either of these pages loads they will load the old existing page,
    // then split the page and continue with new records added to the subsequent
    // append log.

    //
    async _splitLeaf (key, left, cartridges) {
        // Descend to the parent branch page.
        const parent = await this.descend({ key, level: left.level - 1 }, cartridges)

        // Create the right page now so we can lock it. We're going to
        // synchronously add it to the tree and then do the housekeeping to
        // persist the split asynchronously. While we're async, someone could
        // descend the tree and start writing. In fact, this is very likely to
        // happen during a batch insert by the user.
        const right = this._create({
            id: this.storage.nextId(true),
            leaf: true,
            items: [],
            right: null,
            dependents: {},
            key: null,
            log: null
        }, cartridges)

        const messages = []

        // Create our journaled tree alterations.
        const pauses = []
        try {
            pauses.push(await this._fracture.appender.pause(left.page.id))
            pauses.push(await this._fracture.appender.pause(right.page.id))
            // Race is the wrong word, it's our synchronous time. We have to split
            // the page and then write them out. Anyone writing to this leaf has to
            // to be able to see the split so that they surrender their cursor if
            // their insert or delete belongs in the new page, not the old one.
            //
            // Notice that all the page manipulation takes place before the first
            // write. Recall that the page manipulation is done to the page in
            // memory which is offical, the page writes are lagging.

            // Split page creating a right page.
            const length = left.page.items.length
            const partition = Partition(this.comparator.branch, left.page.items)
            // If we cannot partition because the leaf and branch have different
            // partition comparators and the branch comparator considers all keys
            // identical, we give up and return. We will have gone through the
            // housekeeping queue to get here, and if the user keeps inserting keys
            // that are identical according to the branch comparator, we'll keep
            // making our futile attempts to split. Currently, though, we're only
            // going to see this behavior in Amalgamate when someone is staging an
            // update to the same key, say inserting it and deleting it over and
            // over, and then if they are doing it as part of transaction, we'd only
            // attempt once for each batch of writes. We could test the partition
            // before the entry into the housekeeping queue but then we have a
            // racing unit test to write to get this branch to execute, so I won't
            // bother until someone actually complains. It would mean a stage with
            // 100s of updates to one key that occur before the stage can merge
            // before start to his this early exit.
            if (partition == null) {
                cartridges.forEach(cartridge => cartridge.release())
                right.cartridge.remove()
                return
            }
            const items = left.page.items.splice(partition)
            right.page.key = this.comparator.zero(items[0].key)
            right.page.items = items
            right.page.right = left.page.right
            right.cartridge.heft = items.reduce((sum, item) => sum + item.heft, 1)
            // Set the right key of the left page.
            left.page.right = right.page.key
            left.cartridge.heft -= right.cartridge.heft - 1

            // Set the heft of the left page and entry. Moved this down.
            // child.entry.heft -= heft - 1

            // Insert a reference to the right page in the parent branch page.
            // Use an approximate heft for writeahead only storage, recalculated
            // for file system storage.

            //
            parent.page.items.splice(parent.index + 1, 0, {
                key: right.page.key,
                id: right.page.id,
                heft: parent.page.items[parent.page.items.length - 1].heft
            })

            // If any of the pages is still larger than the split threshhold, check
            // the split again.
            for (const page of [ left.page, right.page ]) {
                if (
                    page.items.length >= this.leaf.split &&
                    this.comparator.branch(page.items[0].key, page.items[page.items.length - 1].key) != 0
                ) {
                    this._fracture.housekeeper.enqueue('housekeeping').candidates.push(page.key || page.items[0].key)
                }
            }
            //

            // Write any queued writes, they would have been in memory, in the page
            // that was split above. We based our split on these writes.

            //
            const writes = []
            for (const entries of pauses[0].entries) {
                writes.push.apply(writes, entries.writes.splice(0))
            }
            writes.push.apply(writes)
            //

            this._balanceIf(parent, messages, { method: 'balance', key: key, level: parent.level })

            // Once we await our synchronous operations are over. The user can
            // append new writes to the existing queue entry.
            //
            // All user operations are synchronous, operating on a page after a
            // synchronous descent with no async operations allowed while they
            // hold the page. This means we do not have to worry about splitting
            // a page out from under them.
            //
            // Thus, the first asynchronous action is a synchronous lock release
            // of a sort, the user can now change the page in memory. We have
            // still paused all writes to both the left and right pages and we
            // are in a hurry to release that lock.

            //
            await this.storage.writeSplitLeaf({ key, left, right, parent, writes, messages })
            //
        } finally {
            //

            // **TODO** We probably don't want to release our locks, it just
            // means that work proceeds in some fashion that causes problems,
            // and how will our appender strand know that this strand is in a
            // bad way? Can we have an errored flag on the destructible?

            // We can resume writing. Everything else is going to happen to log
            // files are are not write contended.

            //
            pauses.forEach(pause => pause.resume())
            cartridges.forEach(cartridge => cartridge.release())
            //
        }
        //

        // We run this function to continue balancing the tree.

        //
        await this.storage.balance()
    }

    // **TODO** Something is wrong here. We're using `child.right` to find the a
    // right branch page but the leaf and and it's right sibling can always be
    // under the same branch. How do we really go right?
    //
    // **TODO** The above is a major problem. This is super broken. We may end
    // up merging a page into nothing.
    //
    // **TODO** Regarding the above. Stop and think about it and you can see
    // that you can always pick up the right key of the page at a particular
    // level as you descend the tree. On the way down, update a right variable
    // with the id of the page for the node to the right of the node you
    // followed if one exists. If the page you followed is at the end of the
    // array do not update it. Wait... Is that what `child.right` is here? Heh.
    // It might well be. I see am tracking right as I descend.
    //
    // **TODO** LOL at all that above and if you're smarter when you wrote the
    // code than when you wrote these comments, rewrite all this into a
    // description so you don't do this again.

    //
    async _selectMerger (key, child, entries) {
        const level = child.entry.value.leaf ? -1 : child.level
        const left = await this.descend({ key, level, fork: true }, entries)
        const right = child.right == null
                    ? null
                    : await this.descend({ key: child.right, level }, entries)
        const mergers = []
        if (left != null) {
            mergers.push({
                items: left.entry.value.items,
                key: child.entry.value.key || child.entry.value.items[0].key,
                level: level
            })
        }
        if (right != null) {
            mergers.push({
                items: right.entry.value.items,
                count: right.entry.value.items.length,
                key: child.right,
                level: level
            })
        }
        return mergers
            .filter(merger => this.comparator.branch(merger.items[0].key, merger.items[merger.items.length - 1].key) != 0)
            .sort((left, right) => left.items.length - right.items.length)
            .shift()
    }

    _isDirty (page, sizes) {
        return (
            page.items.length >= sizes.split &&
            this.comparator.branch(page.items[0].key, page.items[page.items.length - 1].key) != 0
        )
        ||
        (
            ! (page.id == '0.1' && page.right == null) &&
            page.items.length <= sizes.merge
        )
    }

    async _surgery (right, pivot) {
        const surgery = {
            deletions: [],
            replacement: null,
            splice: pivot
        }

        // If the pivot is somewhere above we need to promote a key, unless all
        // the branches happen to be single entry branches.
        if (right.level - 1 != pivot.level) {
            let level = right.level - 1
            do {
                const ancestor = this.descend({ key, level }, entries)
                if (ancestor.page.items.length == 1) {
                    surgery.deletions.push(ancestor)
                } else {
                    // TODO Also null out after splice.
                    assert.equal(ancestor.index, 0, 'unexpected ancestor')
                    surgery.replacement = ancestor.page.items[1].key
                    surgery.splice = ancestor
                }
                level--
            } while (surgery.replacement == null && level != right.pivot.level)
        }

        return surgery
    }

    // **TODO** The way that I'm journaling these balances, I need to ensure
    // that I am not journaling a page that will be deleted. Something like
    // right then left then parent, because if we go left to right the left page
    // may choose to merge with its right sibling deleting it. If the right page
    // choose to merge with the left sibling it will delete itself. No, no.
    // We're going by keys, so we're not going to load a deleted page. But, the
    // descent logic depends on nagivating by the least key in the branch page,
    // so we need to be sure to check that we hit the correct key.


    // Easiest way to keep from having a bunch of tests we have to hit..

    // We check as to whether or not to add the merge, so we're not building up
    // a great big list, just... If we are going to try to merge this page
    // again, we will check the parent after we merge again. We have to move
    // merge selection into branch merge so that if we can't merge, we still
    // check the parent. For split we can always check the parent and then the
    // left and right we are only ever adding pages. What about the case where
    // split and then possibly merge? We should see if we shouldn't spam the
    // balance queue and then see if don't luck out and hit the cancel
    // condition.

    // Fill root will delete a child. Plus, we have an ever growing list of
    // possible balance operations so we have to link about what is already in
    // the list.

    //
    async _fillRoot (messages, cartridges) {
        const root = await this.descend({ key: null, level: 0 }, cartridges)
        const child = await this.descend({ key: null, level: 1 }, cartridges)

        root.page.items = child.page.items

        messages.forEach(message => message.level--)

        await this.storage.writeFillRoot({ root, child, messages })
    }

    async _mergeBranch ({ key, level }, messages, cartridges) {
        // **TODO** We don't have to worry. If we go right first, it will have a
        // pivot and if so it has a left, if not it has no left. EXCEPT we just
        // got this from merger selection so we know it is good, what is going
        // on in merger selection?
        const left = await this.descend({ key, level, fork: true }, cartridges)
        const right = await this.descend({ key, level }, cartridges)

        const pivot = await this.descend(right.pivot, cartridges)

        const surgery = await this._surgery(right, pivot)

        right.page.items[0].key = key
        left.page.items.push.apply(left.page.items, right.page.items)

        // Replace the key of the pivot if necessary.
        if (surgery.replacement != null) {
            pivot.page.items[pivot.index].key = surgery.replacement
        }

        // Remove the branch page that references the leaf page.
        surgery.splice.page.items.splice(surgery.splice.index, 1)

        // If the splice index was zero, null the key of the new left most branch.
        if (surgery.splice.index == 0) {
            surgery.splice.page.items[0].key = null
        }
        //

        // **TODO** This needs to be tested. With some confidence in the pivot
        // logic I'm going to use the pivot of the left and the splice to find
        // them. The key for a branch page that is not the right most path
        // always going to be the pivot.

        //
        if (left.pivot == null) {
            messages.push({
                method: 'balance', key: null, level: level
            })
        } else {
            messages.push({
                method: 'balance', key: left.pivot.key, level: level
            })
        }

        if (surgery.splice.pivot == null) {
            messages.push({
                method: 'balance', key: null, level: surgery.splice.level
            })
        } else {
            message.push({
                method: 'balance', key: surgery.splice.pivot.key, level: surgery.splicelevel
            })
        }

        await this.storage.writeMergeBranch({ key, left, right, pivot, surgery })
    }
    //

    // The thing is this. Whenever I fiddle around serious with this code, I'll
    // introduce a bug, I mean, just while editing I'll hit "x" in `vim` and
    // delete a character, and when I run the test I'll get all kinds of evil.

    // What I'm finding now is that there will be infinite loops when I release
    // the pause in the finally block, but the pause enqueues a new entry when
    // you resume it and then the appender sees that the page needs to merge so
    // we come back here. This is only for a programmer error while editing.

    // In practice, though, if there is a failure to write the journal, how do
    // we proceed? Really leaning heavy on leaving the queue paused. The user
    // will know the writes didn't finish, ah, no they won't.

    // Might release the cartridges, but generally feel like we should leave
    // the...

    // Okay, here is where we could start to use the shutdown behavior. We might
    // have a directory and anything that is dirty, we mkdir the name of the
    // dirty page, so we continue to flush, but we stop balancing. Let's do
    // this.

    //
    async _mergeLeaf ({ key, level }) {
        const cartridges = []

        const left = await this.descend({ key, level, fork: true }, cartridges)
        const right = await this.descend({ key, level }, cartridges)

        const pivot = await this.descend(right.pivot, cartridges)

        const surgery = await this._surgery(right, pivot)

        const messages = [{ method: 'balance', key: key, level: surgery.splice.level }]

        const pauses = []
        try {
            pauses.push(await this._fracture.appender.pause(left.page.id))
            pauses.push(await this._fracture.appender.pause(right.page.id))

            // Add the items in the right page to the end of the left page.
            const items = left.page.items
            const merged = right.page.items.splice(0)
            items.push.apply(items, merged)

            // Set right reference of left page.
            left.page.right = right.page.right

            // Adjust heft of left entry.
            left.cartridge.heft += right.cartridge.heft - 1

            // TODO Remove after a while, used only for assertion in `Cache`.
            right.cartridge.heft -= merged.reduce((sum, item) => sum + item.heft, 0)

            // Mark the right page deleted, it will cause `indexOf` in the `Cursor`
            // to return `null` indicating that the user must release the `Cursor`
            // and descend again.
            // **TODO** No longer necessary, right?
            right.page.deleted = true

            // See if the merged page needs to split or merge further.
            if (this._isDirty(left.page, this.leaf)) {
                this._fracture.housekeeper.enqueue('housekeeping').candidates.push(left.entry.value.items[0].key)
            }

            // Replace the key of the pivot if necessary.
            if (surgery.replacement != null) {
                pivot.page.items[pivot.index].key = surgery.replacement
            }

            // Remove the branch page that references the leaf page.
            surgery.splice.page.items.splice(surgery.splice.index, 1)

            if (surgery.splice.index == 0) {
                surgery.splice.page.items[0].key = null
            }
            //

            // Because user updates are synchronous from descent when we go
            // async any user writes will go to our new merged page. We do need
            // to write the existing writes before we perform our merge.

            //
            const writes = { left: [], right: [] }

            for (const entry of pauses[0].entries) {
                writes.left.push.apply(writes.left, entry.writes.splice(0))
            }

            for (const entry of pauses[1].entries) {
                writes.right.push.apply(writes.right, entry.writes.splice(0))
            }
            //
            await this.storage.writeMergeLeaf({ left, right, surgery, pivot, writes, messages })
            //
        } finally {
            pauses.forEach(pause => pause.resume())
            cartridges.forEach(cartridge => cartridge.release())
        }

        await this.storage.balance(messages)
    }

    // `copacetic` could go like this...

    // We do a flat iteration of the tree from `0.1` following the right page.
    // We first go through every directory and ensure that there is no directory
    // named `seen` deleting it if it exists.

    // Then we iterate and mark as `seen` every directory we visit.

    // When we visit we assert that the page is correctly sorted. We then return
    // the items to the user so the user can examine the entries.

    // We then look for any directories that are unseen and assert that they are
    // `merged` files.

    // We can then iterate through the pages again and vacuum all pages that
    // need to be vacuumed. We know assert that the pages are vacuumed
    // correctly, nowhere does a page reference a page outside of its own
    // directory.

    // Then we can look at the unseen pages and see if any of them reference any
    // of the `merged` files. If not we can delete the merged files.

    // TODO Must wait for housekeeping to finish before closing.
    async _keephouse ({ canceled, value: { candidates } }) {
        this._destructible.progress()
        if (canceled) {
            candidates.forEach(candidate => this._canceled.add(candidate))
        } else {
            for (const key of candidates) {
                const cartridges = []
                const child = await this.descend({ key }, cartridges)
                if (child.entry.value.items.length >= this.leaf.split) {
                    await this._splitLeaf(key, child, cartridges)
                } else if (
                    ! (
                        child.entry.value.id == '0.1' && child.entry.value.right == null
                    ) &&
                    child.entry.value.items.length <= this.leaf.merge
                ) {
                    const merger = await this._selectMerger(key, child, cartridges)
                    cartridges.forEach(cartridge => cartridge.release())
                    if (merger != null) {
                        await this._mergeLeaf(merger)
                    }
                } else {
                    entries.forEach(entry => entry.release())
                }
            }
        }
    }
}

module.exports = Sheaf
