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
const fnv = require('./fnv')

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

// Sort function for file names that orders by their creation order.
const appendable = require('./appendable')

// An `Error` type specific to Strata.
const Strata = { Error: require('./error') }

// A latch.
function latch () {
    let capture
    return { unlocked: false, promise: new Promise(resolve => capture = { resolve }), ...capture }
}

function _path (...vargs) {
    return path.join.apply(path, vargs.map(varg => String(varg)))
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

    // Sheaf accepts the destructible and user options passed to `new Strata`
    constructor (destructible, options) {
        Strata.Error.assert(options.turnstile != null, 'OPTION_REQUIRED', { _option: 'turnstile' })
        Strata.Error.assert(options.directory != null, 'OPTION_REQUIRED', { _option: 'directory' })
        assert(destructible.isDestroyedIfDestroyed(options.turnstile.destructible))

        const leaf = coalesce(options.leaf, {})

        this._instance = Sheaf._instance++
        this.leaf = {
            split: coalesce(leaf.split, 5),
            merge: coalesce(leaf.merge, 1)
        }
        const branch = coalesce(options.branch, {})
        this.branch = {
            split: coalesce(branch.split, 5),
            merge: coalesce(branch.merge, 1)
        }
        this.pages = options.pages
        this.handles = options.handles
        this.instance = 0
        this.directory = options.directory
        this._checksum = options.checksum || fnv
        this.serializer = function () {
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
        this.extractor = coalesce(options.extractor, parts => parts[0])
        // **TODO** Dead code.
        if (options.comparator == null) {
        }
        this.comparator = function () {
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
        this.$_recorder = Recorder.create(() => '0')
        this._root = null

        // **TODO** Do not worry about wrapping anymore.
        // Operation id wraps at 32-bits, cursors should not be open that long.
        this._operationId = 0xffffffff


        // Concurrency and work queues. One keyed queue for page writes, the
        // other queue will only use a single key for all housekeeping.

        // **TODO** With `Fracture` we can probably start to do balancing in
        // parallel.
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

        this._id = 0
        this.closed = false
        this.destroyed = false
        this._destructible = destructible
        this._leftovers = []
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
        return this._destructible.exceptional('create', async () => {
            const directory = this.directory
            const stat = await Strata.Error.resolve(fs.stat(directory), 'IO_ERROR')
            Strata.Error.assert(stat.isDirectory(), 'CREATE_NOT_DIRECTORY', { directory })
            const dir = await Strata.Error.resolve(fs.readdir(directory), 'IO_ERROR')
            Strata.Error.assert(dir.every(file => /^\./.test(file)), 'CREATE_NOT_EMPTY', { directory })
            await Strata.Error.resolve(fs.mkdir(this._path('instances')), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('pages')), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance')), 'IO_ERROR')
            this._root = this._create({ id: -1, leaf: false, items: [{ id: '0.0' }] }, [])
            await Strata.Error.resolve(fs.mkdir(this._path('instances', '0'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('page'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance', '0.0'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance', '0.1')), 'IO_ERROR')
            const buffers = [ this._recordify({ length: 1 }), this._recordify({ id: '0.1' }, []) ]
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.0', 'page'), Buffer.concat(buffers), { flags: 'as' }), 'IO_ERROR')
            const zero = this._recordify({ method: '0.0' })
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.1', '0.0'), zero, { flags: 'as' }), 'IO_ERROR')
            const one = this._recordify({ method: 'load', page: '0.1', log: '0.0' })
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.1', '0.1'), one, { flags: 'as' }), 'IO_ERROR')
            const journalist = await Journalist.create(directory)
            journalist.message({ method: 'done', previous: 'create' })
            journalist.mkdir('pages/0.0')
            journalist.mkdir('pages/0.1')
            journalist.rename('balance/0.0/page', 'pages/0.0/page')
            journalist.rename('balance/0.1/0.0', 'pages/0.1/0.0')
            journalist.rename('balance/0.1/0.1', 'pages/0.1/0.1')
            journalist.rmdir('balance/0.0')
            journalist.rmdir('balance/0.1')
            await journalist.prepare()
            await journalist.commit()
            await journalist.dispose()
            this._id++
            this._id++
            this._id++
            return strata
        })
    }

    open (strata) {
        return this._destructible.exceptional('open', async () => {
            // TODO Run commit log on reopen.
            this._root = this._create({ id: -1, items: [{ id: '0.0' }] }, [])
            const dir = await Strata.Error.resolve(fs.readdir(this._path('instances')), 'IO_ERROR')
            const instances = dir
                .filter(file => /^\d+$/.test(file))
                .map(file => +file)
                .sort((left, right) => right - left)
            this.instance = instances[0] + 1
            await Strata.Error.resolve(fs.mkdir(this._path('instances', this.instance)), 'IO_ERROR')
            for (const instance of instances) {
                await Strata.Error.resolve(fs.rmdir(this._path('instances', instance)), 'IO_ERROR')
            }
            return strata
        })
    }

    async _hashable (id) {
        const regex = /^[a-z0-9]+$/
        const dir = await fs.readdir(this._path('pages', id))
        const files = dir.filter(file => regex.test(file))
        assert.equal(files.length, 1, `multiple branch page files: ${id}, ${files}`)
        return files.pop()
    }

    async _appendable (id) {
        const dir = await fs.readdir(this._path('pages', id))
        return dir.filter(file => /^\d+\.\d+$/.test(file)).sort(appendable).pop()
    }

    async _read (id, log) {
        const state = { merged: null, split: null, heft: 0 }
        const page = {
            id,
            leaf: true,
            items: [],
            key: null,
            right: null,
            log: { id: log, page: id, loaded: [], replaceable: false },
            split: null,
            merged: null,
            deletes: 0
        }
        const player = new Player(function () { return '0' })
        const buffer = Buffer.alloc(1024 * 1024)
        for await (const { entries } of io.player(player, this._path('pages', id, log), buffer)) {
            for (const entry of entries) {
                const header = JSON.parse(entry.parts.shift())
                switch (header.method) {
                case 'right': {
                        page.right = this.serializer.key.deserialize(entry.parts)
                        assert(page.right != null)
                    }
                    break
                case 'load': {
                        const { page: previous } = await this._read(header.page, header.log)
                        page.items = previous.items
                        page.log.loaded.push(previous.log)
                    }
                    break
                case 'split': {
                        page.items = page.items.slice(header.index, header.length)
                        if (header.dependent != null) {
                            state.split = header.dependent
                        }
                    }
                    break
                case 'replaceable': {
                        page.log.replaceable = true
                    }
                    break
                case 'merge': {
                        const { page: right } = await this._read(header.page, header.log)
                        page.items.push.apply(page.items, right.items)
                        page.right = right.right
                        page.log.loaded.push(right.log)
                    }
                    break
                case 'insert': {
                        const parts = this.serializer.parts.deserialize(entry.parts)
                        page.items.splice(header.index, 0, {
                            key: this.extractor(parts),
                            parts: parts,
                            heft: entry.sizes.reduce((sum, size) => sum + size, 0)
                        })
                    }
                    break
                case 'delete': {
                        page.items.splice(header.index, 1)
                        // TODO We do not want to vacuum automatically, we want
                        // it to be optional, possibly delayed. Expecially for
                        // MVCC where we are creating short-lived trees, we
                        // don't care that they are slow to load due to splits
                        // and we don't have deletes.
                        page.deletes++
                    }
                    break
                case 'merged': {
                        state.merged = header.page
                    }
                    break
                case 'key': {
                        page.key = this.serializer.key.deserialize(entry.parts)
                        break
                    }
                    break
                }
            }
        }
        state.heft = page.items.reduce((sum, record) => sum + record.heft, 1)
        return { page, ...state }
    }

    async read (id) {
        const leaf = +id.split('.')[1] % 2 == 1
        if (leaf) {
            const { page, heft } = await this._read(id, await this._appendable(id))
            assert(page.id == '0.1' ? page.key == null : page.key != null)
            return { page, heft }
        }
        const player = new Player(function () { return '0' })
        const items = []
        const buffer = Buffer.alloc(1024 * 1024)
        const { gathered, size, length } = await io.play(player, this._path('pages', id, 'page'), buffer, (entry, index) => {
            const header = JSON.parse(entry.parts.shift())
            if (index == 0) {
                return { length: header.length }
            }
            items.push({
                id: header.id,
                key: entry.parts.length != 0 ? this.serializer.key.deserialize(entry.parts) : null
            })
        })
        Strata.Error.assert(length != 0, 'CORRUPT_BRANCH_PAGE')
        Strata.Error.assert(gathered[0].length == items.length, 'CORRUPT_BRANCH_PAGE')
        return { page: { id, leaf, items }, heft: length }
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
            if (descent.index != 0) {
                // The last key we visit is the key for the leaf page, if we're
                // headed to a leaf. We don't have to have the exact leaf key,
                // so if housekeeping is queued up in such a way that a leaf
                // page in the queue is absorbed by a merge prior to its
                // housekeeping inspection, the descent on that key is not going
                // to cause a ruckus. Keys are not going to disappear on us when
                // we're doing branch housekeeping.
                const pivot = descent.pivot
                descent.pivot = {
                    key: entry.value.items[descent.index].key,
                    level: descent.level - 1
                }
                // If we're trying to find siblings we're using an exact key
                // that is definately above the level sought, we'll see it and
                // then go left or right if there is a branch in that direction.
                //
                // TODO Earlier I had this at KILLROY below. And I adjust the
                // level, but I don't reference the level, so it's probably fine
                // here.
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

            // If the page is a leaf, assert that we're looking for a leaf and
            // return the leaf page.
            if (entry.value.leaf) {
                descent.found = index >= 0
                descent.index = index < 0 ? ~index : index
                assert.equal(level, -1, 'could not find branch')
                break
            }

            // If the index is less than zero we didn't find the exact key, so
            // we're looking at the bitwise not of the insertion point which is
            // right after the branch we're supposed to descend, so back it up
            // one.
            descent.index = index < 0 ? ~index - 1 : index

            // We're trying to reach branch and we've hit the level.
            if (level == descent.level) {
                break
            }

            // KILLROY was here.

            descent.level++
        }
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
                : await this._destructible.exceptional('load', load, true)
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
                        entries.push(await this.load(descent.miss))
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

    async _writeLeaf (page, writes) {
        const cartridge = await this.handles.get(this._path('pages', page.id, page.log.id))
        try {
            io.append(cartridge.value, 1024 * 1024, () => writes.shift(), () => Buffer.alloc(0))
        } finally {
            cartridge.release()
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
            await this._writeLeaf(page, writes)
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

    _path (...vargs) {
        vargs.unshift(this.directory)
        return path.resolve.apply(path, vargs.map(varg => String(varg)))
    }

    _nextId (leaf) {
        let id
        do {
            id = this._id++
        } while (leaf ? id % 2 == 0 : id % 2 == 1)
        return String(this.instance) + '.' +  String(id)
    }

    // TODO Why are you using the `_id` for both file names and page ids?
    _filename (id) {
        return `${this.instance}.${this._id++}`
    }

    _serialize (header, parts = []) {
        return this._recordify(header, parts.length != 0 ? this.serializer.parts.serialize(parts) : [])
    }

    _recordify (header, parts = []) {
        return this.$_recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
    }

    async _stub (journalist, { log: { id, page }, entries }) {
        const buffer = Buffer.concat(entries.map(entry => this._recordify(entry.header, entry.parts)))
        await Strata.Error.resolve(fs.mkdir(this._path('balance', page), { recursive: true }), 'IO_ERROR')
        await Strata.Error.resolve(fs.writeFile(this._path('balance', page, id), buffer, { flags: 'as' }), 'IO_ERROR')
        journalist.rename(_path('balance', page, id), _path('pages', page, id))
    }

    async _writeBranch (journalist, branch, create) {
        const filename = this._path('balance', branch.page.id, 'page')
        await Strata.Error.resolve(fs.mkdir(path.dirname(filename), { recursive: true }), 'IO_ERROR')
        const handle = await Strata.Error.resolve(fs.open(filename, 'ax'), 'IO_ERROR')
        const heft = await io.append(handle, 1024 * 1024, index => {
            if (index == branch.page.items.length) {
                return null
            }
            const { id, key } = branch.page.items[index]
            const parts = key != null ? this.serializer.key.serialize(key) : []
            return this._recordify({ id }, parts)
        }, which => {
            if (which == 'header') {
                return this._recordify({ length: branch.page.items.length })
            }
            return Buffer.alloc(0)
        })
        await Strata.Error.resolve(handle.sync(), 'IO_ERROR')
        await Strata.Error.resolve(handle.close(), 'IO_ERROR')
        branch.cartridge.heft = heft
        if (create) {
            journalist.mkdir(_path('pages', branch.page.id))
        } else {
            journalist.unlink(_path('pages', branch.page.id, 'page'))
        }
        journalist.rename(_path('balance', branch.page.id, 'page'), _path('pages', branch.page.id, 'page'))
        journalist.rmdir(_path('balance', branch.page.id))
    }
    //

    // Vacuum is performed after split, merge or rotate. Page logs form a linked
    // list. There is an load instruction at the start of the the log that tells
    // it to load the previous log file.
    //
    // Split, merge and rotate create a new log head. The new log head loads a
    // place holder log. The place holder log contains a load operation to load
    // the old log followed by a split operation for split, a merge operation
    // for merge, or nothing for rotate.

    // Vacuum replaces the place holder with a vacuumed page. The place holder
    // page is supposed to be short lived and conspicuous. We don't want to
    // replace the old log directly. First off, we can't in the case of split,
    // the left and right page share a previous log. Moreover, it just don't
    // seem right. Seems like it would be harder to diagnose problems. With this
    // we'll get a clearer picture of where things failed by leaving more of a
    // trail.

    // We removed dependency tracking from the logs themselves. They used to
    // make note of who depended upon them and we would reference count. It was
    // too much. Now I wonder how we would vacuum if things got messed up. We
    // would probably have to vacuum all the pages, then pass over them for
    // unlinking of old files.

    // We are going to assert dependencies for now, but they will allow us to
    // detect broken and repair pages in the future, should this become
    // necessary. In fact, if we add an instance number we can assert that
    // dependency has not been allowed to escape the journal.

    // Imagining a recovery utility that would load pages, and then check the
    // integrity of a vacuum, calling a modified version of this function with
    // all of the assumptions. That is, explicitly ignore this dependent.

    //
    async _vacuum (journalist, keys, messages) {
        //
        const cartridges = []
        try {
            //

            // Obtain the pages so that they are not read while we are rewriting
            // their log history.

            //
            const unlinkable = new Set

            function unlink (loaded) {
                for (const log of loaded) {
                    unlinkable.add(_path('pages', log.page, log.id))
                    unlink(log.loaded)
                }
            }

            for (const key of keys) {
                const loaded = (await this.descend({ key }, cartridges)).page.log.loaded
                //

                // We want the log history of the page.

                //
                Strata.Error.assert(loaded.length == 1 && loaded[0].replaceable, 'VACUUM_PREVIOUS_NOT_REPLACABLE')
                const log = loaded[0]
                //

                // We don't use the cached page. We read the log starting from the
                // replacable log entry.

                //
                const { page: page, split } = await this._read(log.page, log.id)
                Strata.Error.assert(page.log.replaceable, 'STRANGE_VACUUM_STATE')
                Strata.Error.assert(page.id == log.page, 'STRANGE_VACUUM_STATE')
                if (page.split != null) {
                    const dependent = await this._read(page.split, await this._appendable(page.split))
                    Strata.Error.assert(dependent.log.loaded.length == 0 && !dependent.log.loaded[0].replaceable, 'UNVACUUMED_DEPENDENT')
                }
                //

                // Write the entries in order. Any special commands like `'key'`
                // or `'right'` have been written into the the new head log.

                // Fun fact: if you want an exception you could never catch, an
                // `fs.WriteStream` with something other than a number a `fd` like a
                // `string`. You'll see that an assertion is raised where it can
                // never be caught. We don't use `fs.WriteStream` anymore, though.

                //
                await Strata.Error.resolve(fs.mkdir(this._path('balance', page.id)), 'IO_ERROR')
                const handle = await Strata.Error.resolve(fs.open(this._path('balance', page.id, page.log.id), 'a'), 'IO_ERROR')
                await io.append(handle, 1024 * 1024, index => {
                    if (index == page.items.length) {
                        return null
                    }
                    const parts = this.serializer.parts.serialize(page.items[index].parts)
                    return this._recordify({ method: 'insert', index }, parts)
                }, () => Buffer.alloc(0))

                await Strata.Error.resolve(handle.sync(), 'IO_ERROR')
                await Strata.Error.resolve(handle.close(), 'IO_ERROR')

                messages.forEach(message => journalist.message(message))
                journalist.unlink(_path('pages', page.id, page.log.id))
                journalist.rename(_path('balance', page.id, page.log.id), _path('pages', page.id, page.log.id))
                journalist.rmdir(_path('balance', page.id))

                unlink(page.log.loaded)
            }

            for (const unlink of unlinkable) {
                journalist.unlink(unlink)
            }

            await journalist.prepare()
            await journalist.commit()
        } finally {
            cartridges.forEach(cartridge => cartridge.release())
        }

        await this._journal()
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
    async _drainRoot (journalist, key) {
        const cartridges = []

        const root = await this.descend({ key, level: 0 }, cartridges)

        const partition = Math.floor(root.entry.value.items.length / 2)

        const left = this._create({
            id: this._nextId(false),
            offset: 1,
            items: root.page.items.slice(0, partition),
            hash: null
        }, cartridges)

        const right = this._create({
            id: this._nextId(false),
            offset: 1,
            items: root.page.items.slice(partition),
            hash: null
        }, cartridges)

        root.page.items = [{
            id: left.page.id,
            key: null
        }, {
            id: right.page.id,
            key: right.page.items[0].key
        }]
        right.page.items[0].key = null

        await this._writeBranch(journalist, right, true)
        await this._writeBranch(journalist, left, true)
        await this._writeBranch(journalist, root, false)

        journalist.message({ method: 'balance', key: key, level: 1 })
        journalist.message({ method: 'balance', key: right.page.items[0].key, level: 1 })

        await journalist.prepare()
        await journalist.commit()
        await journalist.dispose()

        cartridges.forEach(cartridge => cartridge.release())

        await this._journal()
    }

    async _balance (journalist, key, level, messages) {
        messages.forEach(message => journalist.message(message))
        const cartridges = []
        const branch = await this.descend({ key, level }, cartridges)
        cartridges.forEach(cartridge => cartridge.release())
        const leaves = +branch.page.items[0].id.split('.')[1] % 2 == 1
        if (branch.page.items.length >= this.branch.split) {
            if (branch.page.id == '0.0') {
                await this._drainRoot(journalist, key, messages)
            } else {
                await this._splitBranch(journalist, key, level, messages)
            }
        } else if (branch.page.items.length <= this.branch.merge) {
            if (branch.page.id != '0.0') {
                // TODO Have `_selectMerger` manage its own entries.
                const cartridges = []
                const merger = await this._selectMerger(key, branch, cartridges)
                cartridges.forEach(cartridge => cartridge.release())
                await this._mergeBranch(journalist, merger, messages)
            } else if (! leaves && branch.page.items.length == 1) {
                await this._fillRoot(journalist, key, messages)
            } else {
                await journalist.prepare()
                await journalist.commit()
                await this._journal()
            }
        } else  {
            await journalist.prepare()
            await journalist.commit()
            await this._journal()
        }
    }

    async _splitBranch (journalist, key, level) {
        const cartridges = []
        const left = await this.descend({ key, level }, cartridges)
        const parent = await this.descend({ key, level: level - 1 }, cartridges)

        const partition = Math.floor(left.page.items.length / 2)

        const right = this._create({
            id: this._nextId(false),
            items: left.page.items.splice(partition),
            leaf: false
        }, cartridges)

        const promotion = right.page.items[0].key
        right.page.items[0].key = null
        left.page.items = left.page.items.splice(0, partition)
        parent.page.items.splice(parent.index + 1, 0, { key: promotion, id: right.page.id })

        await this._writeBranch(journalist, left, false)
        await this._writeBranch(journalist, right, true)
        await this._writeBranch(journalist, parent, false)

        journalist.message({ method: 'balance', key: key, level: level - 1 })
        journalist.message({ method: 'balance', key: key, level: level })
        journalist.message({ method: 'balance', key: right.page.items[0].key, level: level })

        await journalist.prepare()
        await journalist.commit()

        cartridges.forEach(cartridge => cartridge.release())

        await this._journal()
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
            id: this._nextId(true),
            leaf: true,
            items: [],
            right: null,
            dependents: {},
            key: null,
            log: null
        }, cartridges)

        // Create our journaled tree alterations.
        const journalist = await Journalist.create(this.directory)
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
            parent.page.items.splice(parent.index + 1, 0, {
                key: right.page.key,
                id: right.page.id,
                // TODO For branches, let's always just re-run the sum.
                heft: 0
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
            await this._writeLeaf(left.page, writes)
            //

            // Create the new page directory in our journal.

            //
            journalist.mkdir(_path('pages', right.page.id))
            //

            // Pages are broken up into logs. The logs have a load instruction
            // that will tell them to load a previous log, essentially a linked
            // list. They have a split (**TODO** rename) instruction that will
            // tell them to split the page they loaded. When reading it will
            // load the new head which will tell it to load the previous page
            // and split it.

            // Except we don't want to have an indefinate linked list. We vacuum
            // when we split. We do this by inserting a place holder log between
            // the old log and the new log. The place holder conatains just the
            // load and split operation. After these two small files are written
            // and synced, we can release our pause on writes on the cache page
            // and move onto vacuum.

            // New writes will go to the head of the log. We will replace our
            // place-holder with a vacuumed copy of the previous log each page
            // receiving just its half of the page will all delete operations
            // removed. When we vacuum we only need to hold a cache reference to
            // the page so it will not be evicted and re-read while we're
            // moving the old logs around, so vacuuming can take place in
            // parallel to all user operations.
            //
            // The replacement log will also include an indication of
            // dependency. It will mark a `split` property in the page for the
            // left page. During vacuum the we will check the `split` property
            // of the page created by reading the replacable part of the log. If
            // it is not null we will assert that the dependent page is vacuumed
            // exist before we vacuum. This means we must vacuum the right page
            // befroe we vacuum the left page.

            const replace = {
                left: {
                    log: {
                        id: this._filename(),
                        page: left.page.id,
                        loaded: [ left.page.log ],
                        replaceable: true
                    },
                    entries: [{
                        header: {
                            method: 'load',
                            page: left.page.id,
                            log: left.page.log.id
                        }
                    }, {
                        header: {
                            method: 'split',
                            index: 0,
                            length: partition,
                            dependent: right.page.id
                        }
                    }, {
                        header: {
                            method: 'replaceable'
                        }
                    }]
                },
                right: {
                    log: {
                        id: this._filename(),
                        page: right.page.id,
                        loaded: [ left.page.log ],
                        replaceable: true
                    },
                    entries: [{
                        header: {
                            method: 'load',
                            page: left.page.id,
                            log: left.page.log.id
                        }
                    }, {
                        header: {
                            method: 'split',
                            index: partition,
                            length: length,
                            dependent: null
                        }
                    }, {
                        header: { method: 'replaceable' }
                    }]
                }
            }

            await this._stub(journalist, replace.left)
            await this._stub(journalist, replace.right)
            //

            // Write the new log head which loads our soon to be vacuumed place
            // holder.

            //
            const stub = {
                left: {
                    log: {
                        id: this._filename(),
                        page: left.page.id,
                        loaded: [ replace.left.log ],
                        replaceable: false
                    },
                    entries: [{
                        header: {
                            method: 'load',
                            page: replace.left.log.page,
                            log: replace.left.log.id
                        }
                    }, {
                        header: { method: 'right' },
                        parts: this.serializer.key.serialize(right.page.key)
                    }]
                },
                right: {
                    log: {
                        id: this._filename(),
                        page: right.page.id,
                        loaded: [ replace.right.log ],
                        replaceable: false
                    },
                    entries: [{
                        header: {
                            method: 'load',
                            page: replace.right.log.page,
                            log: replace.right.log.id
                        }
                    }, {
                        header: { method: 'key' },
                        parts: this.serializer.key.serialize(right.page.key)
                    }]
                }
            }

            if (left.page.id != '0.1') {
                stub.left.entries.push({
                    header: { method: 'key' },
                    parts: this.serializer.key.serialize(left.page.key)
                })
            }

            if (right.page.right != null) {
                stub.right.entries.push({
                    header: { method: 'right' },
                    parts: this.serializer.key.serialize(right.page.right)
                })
            }

            await this._stub(journalist, stub.left)
            await this._stub(journalist, stub.right)
            //

            // Update the log history. **TODO** Maybe we rename this to log
            // instead of append?

            //
            left.page.log = stub.left.log
            right.page.log = stub.right.log
            //

            // Update the left page's dependents.

            //

            // We record the new node in our parent branch.

            //
            await this._writeBranch(journalist, parent, false)
            //

            // Delete our scrap directories.

            //
            journalist.rmdir(_path('balance', left.page.id))
            journalist.rmdir(_path('balance', right.page.id))
            //

            // Here we add messages to our journal saying what we want to do
            // next. We run a journal for each step.

            //
            journalist.message({ method: 'vacuum', keys: [ right.page.key, key ] })
            journalist.message({ method: 'balance', key: key, level: parent.level })
            //

            // Run the journal, prepare it and commit it. If prepare fails the
            // split never happened, we'll split the page the next time we visit
            // it. If commit fails everything we did above will happen in
            // recovery.

            //
            await journalist.prepare()
            await journalist.commit()
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
        await this._journal()
    }

    async _rmrf (journalist, pages, messages) {
        for (const id of pages) {
            const leaf = +id.split('.')[1] % 2 == 1
            if (leaf) {
                const { page, merged } = await this._read(id, await this._appendable(id))
                Strata.Error.assert(merged != null, 'DELETING_UNMERGED_PAGE')
                await fs.rmdir(this._path('pages', id), { recursive: true })
            } else {
                await fs.rmdir(this._path('pages', id), { recursive: true })
            }
        }
        messages.forEach(message => journalist.message(message))
        await journalist.prepare()
        await journalist.commit()
        await this._journal()
    }

    async _journal () {
        const journalist = await Journalist.create(this.directory)
        const messages = journalist.messages.slice(0)
        console.log(messages)
        if (messages.length != 0) {
            const message = messages.shift()
            switch (message.method) {
            case 'vacuum':
                await this._vacuum(journalist, message.keys, messages)
                break
            case 'balance':
                console.log('visit')
                await this._balance(journalist, message.key, message.level, messages)
                break
            case 'rmrf':
                await this._rmrf(journalist, message.pages, messages)
                break
            }
        }
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

    async _fillRoot (journalist) {
        const cartridges = []

        const root = await this.descend({ key: null, level: 0 }, cartridges)
        const child = await this.descend({ key: null, level: 1 }, cartridges)

        root.page.items = child.page.items

        // Write the merged page.
        await this._writeBranch(journalist, root, false)

        journalist.unlink(_path('pages', child.page.id, 'page'))
        journalist.rmdir(_path('pages', child.page.id))

        // Record the commit.
        await journalist.prepare()
        await journalist.commit()

        cartridges.forEach(cartridge => cartridge.release())

        await this._journal()
    }

    async _possibleMerge (surgery, key, branch) {
        if (surgery.splice.entry.value.items.length <= this.branch.merge) {
            if (surgery.splice.entry.value.id != '0.0') {
            } else if (branch && this.branch.merge == 1) {
                await this._fill(key)
            }
        }
    }

    async _mergeBranch (journalist, { key, level }) {
        const cartridges = []

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

        // Write the merged page.
        await this._writeBranch(journalist, left, false)

        // Delete the page merged into the merged page.
        journalist.unlink(_path('pages', right.page.id, 'page'))
        journalist.rmdir(_path('pages', right.page.id))

        // If we replaced the key in the pivot, write the pivot.
        if (surgery.replacement != null) {
            await this._writeBranch(journalist, pivot, false)
        }

        // Write the page we spliced.
        await this._writeBranch(journalist, surgery.splice, false)

        // Delete any removed branches.
        for (const deletion in surgery.deletions) {
            throw new Error
            await commit.unlink(path.join('pages', deletion.entry.value.id))
        }
        //

        // TODO This needs to be tested.

        console.log('visit')

        //
        if (left.pivot == null) {
            journalist.message({ method: 'balance', key: null, level: level })
            journalist.message({ method: 'balance', key: null, level: level - 1 })
        } else {
            journalist.message({ method: 'balance', key: left.pivot.key, level: level })
            journalist.message({ method: 'balance', key: null, level: level - 1 })
        }

        // Record the commit.
        await journalist.prepare()
        await journalist.commit()

        cartridges.forEach(cartridge => cartridge.release())

        await this._journal()
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

        // Create our journaled tree alterations.
        const journalist = await Journalist.create(this.directory)

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

            await this._writeLeaf(left.page, writes.left)
            await this._writeLeaf(right.page, writes.right)
            //

            // We discuss this in detail in `_splitLeaf`. We want a record of
            // dependents and we probably want that to be in the page directory
            // of each page if we're going to do some sort of audit that
            // includes a directory scan looking for orphans.

            // We know that the left page into which we merged already has a
            // dependent record so we need to add one...

            // Maybe we do not have a dependent record that references the self,
            // only the other. This makes more sense. It would be easier to test
            // that dependents are zero. There is only ever one dependent record
            // and if it the same page as the loaded page it is a merge,
            // otherwise it is a split.

            //
            const terminator = {
                log: {
                    id: this._filename(),
                    page: right.page.id,
                    loaded: [ right.page.log ],
                    replaceable: false
                },
                entries: [{
                    header: {
                        method: 'merged',
                        page: left.page.id
                    }
                }]
            }

            await this._stub(journalist, terminator)

            const replace = {
                log: {
                    id: this._filename(),
                    page: left.page.id,
                    loaded: [ left.page.log, right.page.log ],
                    replaceable: true
                },
                entries: [{
                    header: {
                        method: 'load',
                        page: left.page.id,
                        log: left.page.log.id
                    }
                }, {
                    header: {
                        method: 'merge',
                        page: right.page.id,
                        log: right.page.log.id
                    }
                }, {
                    header: {
                        method: 'replaceable'
                    }
                }]
            }

            await this._stub(journalist, replace)

            const stub = {
                log: {
                    id: this._filename(),
                    page: left.page.id,
                    loaded: [ replace.log ],
                    replaceable: false
                },
                entries: [{
                    header: {
                        method: 'load',
                        page: replace.log.page,
                        log: replace.log.id
                    }
                }]
            }

            if (left.page.id != '0.1') {
                stub.entries.push({
                    header: { method: 'key' },
                    parts: this.serializer.key.serialize(left.page.key)
                })
            }

            if (right.page.right != null) {
                stub.entries.push({
                    header: { method: 'right' },
                    parts: this.serializer.key.serialize(right.page.right)
                })
            }

            await this._stub(journalist, stub)

            // If we replaced the key in the pivot, write the pivot.
            if (surgery.replacement != null) {
                await this._writeBranch(journalist, pivot, false)
            }

            // Write the page we spliced.
            await this._writeBranch(journalist, surgery.splice, false)

            left.page.log = stub.log
            right.page.log = terminator.log

            journalist.message({ method: 'vacuum', keys: [ key ] })
            journalist.message({
                method: 'rmrf',
                pages: surgery.deletions.map(deletion => deletion.page.id).concat(right.page.id)
            })
            journalist.message({ method: 'balance', key: left.page.key, level: surgery.splice.level, x: 1 })
            //

            // Delete our scrap directories.

            //
            journalist.rmdir(_path('balance', left.page.id))
            journalist.rmdir(_path('balance', right.page.id))
            //

            // Record the commit.
            await journalist.prepare()
            await journalist.commit()
        } finally {
            pauses.forEach(pause => pause.resume())
            cartridges.forEach(cartridge => cartridge.release())
        }

        await this._journal()
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
