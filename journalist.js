const ascension = require('ascension')
const fileSystem = require('fs')
const fs = require('fs').promises
const path = require('path')
const recorder = require('./recorder')
const Player = require('./player')
const find = require('./find')
const assert = require('assert')
const Cursor = require('./cursor')
const callback = require('prospective/callback')
const coalesce = require('extant')
const Future = require('prospective/future')
const Commit = require('./commit')
const fnv = require('./fnv')

const Turnstile = require('turnstile')
Turnstile.Queue = require('turnstile/queue')
Turnstile.Set = require('turnstile/set')

function traceIf (condition) {
    if (condition) return function (...vargs) {
        console.log.apply(console, vargs)
    }
    return function () {}
}

const appendable = require('./appendable')

const Strata = { Error: require('./error') }

class Journalist {
    constructor (destructible, options) {
        const leaf = coalesce(options.leaf, {})
        this.leaf = {
            split: coalesce(leaf.split, 5),
            merge: coalesce(leaf.merge, 1)
        }
        const branch = coalesce(options.branch, {})
        this.branch = {
            split: coalesce(branch.split, 5),
            merge: coalesce(branch.merge, 1)
        }
        this.cache = options.cache
        this.instance = 0
        this.directory = options.directory
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
                            return Buffer.from(JSON.stringify(key))
                        },
                        deserialize: function (key) {
                            return JSON.parse(key.toString())
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
                        serialize: function (part) { return part },
                        deserialize: function (part) { return part }
                    }
                }
            default:
                return serializer
            }
        } ()
        this.extractor = coalesce(options.extractor, parts => parts[0])
        this.comparator = coalesce(options.comparator, ascension([ String ], (value) => [ value ]))
        this._recorder = recorder(() => '0')
        this._root = null
        // Operation id wraps at 32-bits, cursors should not be open that long.
        this._operationId = 0xffffffff
        const turnstiles = Math.min(coalesce(options.turnstiles, 3), 3)
        const appending = new Turnstile(destructible.durable('appender'), { turnstiles })
        // TODO Convert to Turnstile.Set.
        this._appending = new Turnstile.Queue(appending, this._append, this)
        this._queues = {}
        this._blocks = [{}]
        const housekeeping = new Turnstile(destructible.durable('housekeeper'))
        this._housekeeping = new Turnstile.Set(housekeeping, this._housekeeper, this)
        this._id = 0
        this.closed = false
        this.destroyed = false
        destructible.destruct(() => this.destroyed = true)
    }

    async create () {
        const directory = this.directory, stat = await fs.stat(directory)
        Strata.Error.assert(stat.isDirectory(), 'create.not.directory', { directory })
        Strata.Error.assert((await fs.readdir(directory)).filter(file => {
            return ! /^\./.test(file)
        }).length == 0, 'create.directory.not.empty', { directory })

        this._root = this._create({ id: -1, items: [{ id: '0.0' }] })

        await fs.mkdir(this._path('instance', '0'), { recursive: true })
        await fs.mkdir(this._path('pages', '0.0'), { recursive: true })
        const buffer = Buffer.from(JSON.stringify([{ id: '0.1', key: null }]))
        const hash = fnv(buffer)
        await fs.writeFile(this._path('pages', '0.0', hash), buffer)
        await fs.mkdir(this._path('pages', '0.1'), { recursive: true })
        await fs.writeFile(this._path('pages', '0.1', '0.0'), Buffer.alloc(0))
    }

    async open () {
        this._root = this._create({ id: -1, items: [{ id: '0.0' }] })
        const instances = (await fs.readdir(this._path('instances')))
            .filter(file => /^\d+$/.test(file))
            .map(file => +file)
            .sort((left, right) => right - left)
        this.instance = instances[0] + 1
        await fs.mkdir(this._path('instances', this.instance))
        for (let instance of instances) {
            await fs.rmdir(this._path('instances', instance))
        }
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

    async _read (id, append) {
        const page = {
            id,
            leaf: true,
            items: [],
            entries: [],
            deletes: 0,
            // TODO Rename merged.
            deleted: false,
            lock: null,
            right: null,
            ghosts: 0,
            append
        }
        const player = new Player(function () { return '0' })
        const readable = fileSystem.createReadStream(this._path('pages', id, append))
        for await (let chunk of readable) {
            for (let entry of player.split(chunk)) {
                switch (entry.header.method) {
                case 'right': {
                        // TODO Need to use the key section of the record.
                        page.right = this.serializer.key.deserialize(entry.parts[0])
                        assert(page.right != null)
                    }
                    break
                case 'load': {
                        const { id, append } = entry.header
                        const { page: loaded } = await this._read(id, append)
                        page.items = loaded.items
                        page.right = loaded.right
                        page.entries.push({
                            method: 'load', header: entry.header, entries: loaded.entries
                        })
                    }
                    break
                case 'slice': {
                        if (entry.header.length < page.items.length) {
                            page.right = page.items[entry.header.length].key
                        }
                        page.items = page.items.slice(entry.header.index, entry.header.length)
                    }
                    break
                case 'merge': {
                        const { page: right } = await this._read(entry.header.id, entry.header.append)
                        page.items.push.apply(page.items, right.items.slice(right.ghosts))
                        page.right = right.right
                        page.entries.push({
                            method: 'merge', header: entry.header, entries: right.entries
                        })
                    }
                    break
                case 'insert': {
                        const parts = this.serializer.parts.deserialize(entry.parts)
                        page.items.splice(entry.header.index, 0, {
                            key: this.extractor(parts),
                            parts: parts,
                            heft: entry.sizes.reduce((sum, size) => sum + size, 0)
                        })
                    }
                    break
                case 'delete': {
                        if (entry.header.index == 0) {
                            page.ghosts = 1
                        } else {
                            page.items.splice(entry.header.index, 1)
                        }
                        page.deletes++
                    }
                    break
                case 'dependent': {
                        page.entries.push(entry.header)
                    }
                    break
                }
            }
        }
        const heft = page.items.reduce((sum, record) => sum + record.heft, 0)
        return { page, heft }
    }

    async read (id) {
        const leaf = +id.split('.')[1] % 2 == 1
        if (leaf) {
            return this._read(id, await this._appendable(id))
        }
        const hash = await this._hashable(id)
        const buffer = await fs.readFile(this._path('pages', id, hash))
        const actual = fnv(buffer)
        Strata.Error.assert(actual == hash, 'bad branch hash', {
            id, actual, expected: hash
        })
        const items = JSON.parse(buffer.toString())
        return { page: { id, leaf, items, hash }, heft: buffer.length }
    }

    // What is going on here? Why is there an `entry.heft` and an
    // `entry.value.heft`?

    //
    async load (id) {
        const entry = this._hold(id)
        if (entry.value == null) {
            const { page, heft } = await this.read(id)
            entry.value = page
            entry.heft = heft
        }
        return entry
    }

    _create (page) {
        return this.cache.hold([ this.directory, page.id ], page)
    }

    _hold (id) {
        return this.cache.hold([ this.directory, id ], null)
    }

    // TODO If `key` is `null` then just go left.
    _descend (entries, { key, level = -1, fork = false, rightward = false, approximate = false }) {
        const descent = { miss: null, keyed: null, level: 0, index: 0, entry: null }
        let entry = null
        entries.push(entry = this._hold(-1))
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
                if (descent.pivot.key == key && fork) {
                    descent.index--
                    rightward = true
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
            entries.push(entry = this._hold(id))
            if (entry.value == null) {
                entries.pop().remove()
                return { miss: id }
            }

            // Binary search the page for the key, or just go right or left
            // directly if there is no key.
            const offset = entry.value.leaf ? entry.value.ghosts : 1
            const index = rightward
                ? entry.value.items.length - 1
                : key != null
                    ? find(this.comparator, entry.value, key, offset)
                    : 0

            // If the page is a leaf, assert that we're looking for a leaf and
            // return the leaf page.
            if (entry.value.leaf) {
                descent.index = index
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
                descent.index < 0 ? descent.index++ : descent.index--
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
    async descend (query, entries = []) {
        const _entries = [[]]
        for (;;) {
            _entries.push([])
            const descent = this._descend(_entries[1], query)
            _entries.shift().forEach(entry => entry.release())
            if (descent == null) {
                _entries.shift().forEach((entry) => entry.release())
                return null
            }
            if (descent.miss == null) {
                entries.push(descent.entry = _entries[0].pop())
                _entries.shift().forEach(entry => entry.release())
                return descent
            }
            _entries[0].push(await this.load(descent.miss))
        }
    }

    async close () {
        if (!this.destroyed && !this.closed) {
            assert(!this.destroyed, 'already destroyed')
            this.closed = true
            // Trying to figure out how to wait for the Turnstile to drain. We
            // can't terminate the housekeeping turnstile then the acceptor
            // turnstile because they depend on each other, so we're going to
            // loop. We wait for one to drain, then the other, then check to see
            // if anything is in the queues to determine if we can leave the
            // loop. Actually, we only need to check the size of the first queue
            // in the loop, the second will be empty when `drain` returns.
            do {
                await this._housekeeping.turnstile.drain()
                await this._appending.turnstile.drain()
            } while (this._housekeeping.turnstile.size != 0)
            await this._housekeeping.turnstile.terminate()
            await this._appending.turnstile.terminate()
            if (this._root != null) {
                this._root.remove()
                this._root = null
            }
        }
    }

    async _writeLeaf (id, writes) {
        const append = await this._appendable(id)
        const recorder = this._recorder
        const entry = this._hold(id)
        const buffers = writes.map(write => {
            const buffer = recorder(write.header, write.parts)
            // TODO Where is heft removal?
            if (write.header.method == 'insert') {
                entry.heft += (write.record.heft = buffer.length)
            }
            return buffer
        })
        entry.release()
        await fs.appendFile(this._path('pages', id, append), Buffer.concat(buffers))
    }

    // TODO Not difficult, merge queue and block. If you want to block, then
    // when you get the queue, push promise onto a blocks queue, or simply
    // assign a block. Or, add a block class, { appending: <promise>, blocking:
    // <promise> } where appending is flipped when it enters the abend class and
    // blocking is awaited, and blocking can be left null.
    _queue (id) {
        let queue = this._queues[id]
        if (queue == null) {
            queue = this._queues[id] = {
                id: this._operationId = (this._operationId + 1 & 0xffffffff) >>> 0,
                writes: [],
                entry: this._hold(id),
                written: false,
                promise: this._appending.enqueue({ id }, this._index(id))
            }
        }
        return queue
    }

    // Block writing to a leaf. We do this by adding a block object to the next
    // write that will be pulled from the append queue. This append function
    // will notify that it has received the block by resolving the `enter`
    // future and then wait on the `Promise` of the `exit` `Future`. We will
    // only ever invoke `_block` from our housekeeping thread and so we assert
    // that we've not blocked the same page twice. The housekeeping logic is
    // particular about the leaves it blocks, so it should never overlap itself,
    // and there should never be another strand trying to block appends. We will
    // lock at most two pages at once so we must always have at least two
    // turnstiles running for the `_append` `Turnstile`.

    //
    _block (id) {
        const queue = this._queue(id)
        assert(queue.block == null)
        return queue.block = { enter: new Future, exit: new Future }
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
    async _append ({ body: { id } }) {
        // TODO Doesn't `await null` do the same thing now?
        await callback((callback) => process.nextTick(callback))
        const queue = this._queues[id]
        delete this._queues[id]
        if (queue.block != null) {
            queue.block.enter.resolve()
            await queue.block.exit.promise
        }
        // We flush a page's writes before we merge it into its left
        // sibling so there will always a queue entry for a page that has
        // been merged. It will never have any writes so we can skip writing
        // and thereby avoid putting it back into the housekeeping queue.
        if (queue.writes.length != 0) {
            const page = queue.entry.value
            if (
                page.items.length >= this.leaf.split ||
                (
                    ! (page.id == '0.1' && page.right == null) &&
                    page.items.length <= this.leaf.merge
                )
            ) {
                this._housekeeping.add(page.items[0].key)
            }
            await this._writeLeaf(id, queue.writes)
        }
        queue.entry.release()
        queue.written = true
    }

    _index (id) {
        return id.split('.').reduce((sum, value) => sum + +value, 0) % this._appending.turnstile.health.turnstiles
    }

    append (entry, writes) {
        const queue = this._queue(entry.id)
        queue.writes.push(entry)
        if (writes[queue.id] == null) {
            writes[queue.id] = queue
        }
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

    async _vacuum (key) {
        const entries = []
        const leaf = await this.descend({ key })

        const block = this._block(leaf.entry.value.id)
        await block.enter.promise

        const items = leaf.entry.value.items.slice(0)

        const first = this._filename()
        const second = this._filename()

        const dependencies = function map ({ id, append }, entries, dependencies = {}) {
            assert(dependencies[`${id}/${append}`] == null)
            const page = dependencies[`${id}/${append}`] = {}
            for (const entry of entries) {
                switch (entry.header.method) {
                case 'load':
                case 'merge': {
                        map(entry.header, entry.entries, dependencies)
                    }
                    break
                case 'dependent': {
                        const { id, append } = entry.header
                        assert(!page[`${id}/${append}`])
                        page[`${id}/${append}`] = true
                    }
                    break
                }
            }
            return dependencies
        } (leaf.entry.value, leaf.entry.value.entries)

        await (async () => {
            // Flush any existing writes. We're still write blocked.
            const writes = this._queue(leaf.entry.value.id).writes.splice(0)
            await this._writeLeaf(leaf.entry.value.id, writes)

            // Create our journaled tree alterations.
            const prepare = []

            // Create a stub that loads the existing page.
            const previous = leaf.entry.value.append
            prepare.push({
                method: 'stub',
                page: { id: leaf.entry.value.id, append: first },
                records: [{
                    method: 'load',
                    id: leaf.entry.value.id,
                    append: previous
                }, {
                    method: 'dependent',
                    id: leaf.entry.value.id,
                    append: second
                }]
            }, {
                method: 'stub',
                page: { id: leaf.entry.value.id, append: second },
                records: [{
                    method: 'load',
                    id: leaf.entry.value.id,
                    append: first
                }]
            })
            leaf.entry.value.append = second
            leaf.entry.value.entries = [{
                header: { method: 'load', id: leaf.entry.value.id, append: first },
                entries: [{
                    header: { hmethod: 'dependent', id: leaf.entry.value.id, append: second }
                }]
            }]

            const commit = new Commit(this)
            await commit.write(prepare)
            await commit.prepare()
            await commit.commit()
            await commit.dispose()
        }) ()

        block.exit.resolve()

        await (async () => {
            const prepare = []
            const commit = new Commit(this)

            prepare.push({
                method: 'unlink',
                path: path.join('pages', leaf.entry.value.id, first)
            })

            prepare.push(await commit.vacuum(leaf.entry.value.id, first, second, items, leaf.entry.value.right))
            // Merged pages themselves can just be deleted, but when we do, we
            // need to... Seems like both split and merge can use the same
            // mechanism, this dependent reference. So, every page we load has a
            // list of dependents. We can eliminate any that we know we can
            // delete.

            // Delete previous versions. Oof. Split means we have multiple
            // references.
            const deleted = {}
            const deletions = {}

            // Could save some file operations by maybe doing the will be deleted
            // removals first, but this logic is cleaner.
            for (const page in dependencies) {
                for (const dependent in dependencies[page]) {
                    const [ id, append ] = dependent.split('/')
                    try {
                        await fs.stat(this._path('pages', id, append))
                    } catch (error) {
                        Strata.Error.assert(error.code == 'ENOENT', 'vacuum.not.enoent', error, { id, append })
                        deleted[dependent] = true
                    }
                }
            }

            let loop = true
            while (loop) {
                loop = false
                for (const page in dependencies) {
                    if (Object.keys(dependencies[page]).length == 0) {
                        loop = true
                        deleted[page] = true
                        deletions[page] = true
                        delete dependencies[page]
                    } else {
                        for (const dependent in dependencies[page]) {
                            if (deleted[dependent]) {
                                loop = true
                                delete dependencies[page][dependent]
                            }
                        }
                    }
                }
            }

            // Delete all merged pages.
            for (const deletion in deletions) {
                const [ id, append ] = deletion.split('/')
                prepare.push({
                    method: 'unlink',
                    path: path.join('pages', id, append)
                })
            }

            await commit.write(prepare)
            await commit.prepare()
            await commit.commit()
            await commit.dispose()
        }) ()

        leaf.entry.release()
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
    async _drainRoot (key) {
        const entries = []
        const root = await this.descend({ key, level: 0 }, entries)
        const partition = Math.floor(root.entry.value.items.length / 2)
        // TODO Print `root.page.items` and see that heft is wrong in the items.
        // Why is it in the items and why is it wrong? Does it matter?
        const left = this._create({
            id: this._nextId(false),
            offset: 1,
            items: root.entry.value.items.slice(0, partition),
            hash: null
        })
        entries.push(left)
        const right = this._create({
            id: this._nextId(false),
            offset: 1,
            items: root.entry.value.items.slice(partition),
            hash: null
        })
        entries.push(right)
        root.entry.value.items = [{
            id: left.value.id,
            key: null
        }, {
            id: right.value.id,
            key: right.value.items[0].key
        }]
        right.value.items[0].key = null
        const commit = new Commit(this)
        const prepare = []
        // Write the new branch to a temporary file.
        prepare.push(await commit.emplace(right))
        prepare.push(await commit.emplace(left))
        prepare.push(await commit.emplace(root.entry))
        // Record the commit.
        await commit.write(prepare)
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        entries.forEach(entry => entry.release())
    }

    async _possibleSplit (page, key, level) {
        if (page.items.length >= this.branch.split) {
            if (page.id == '0.0') {
                await this._drainRoot(key)
            } else {
                await this._splitBranch(key, level)
            }
        }
    }

    async _splitBranch (key, level) {
        const entries = []
        const branch = await this.descend({ key, level }, entries)
        const parent = await this.descend({ key, level: level - 1 }, entries)
        const partition = Math.floor(branch.entry.value.items.length / 2)
        const right = this._create({
            id: this._nextId(false),
            leaf: false,
            items: branch.entry.value.items.splice(partition),
            heft: 0,
            hash: null
        })
        entries.push(right)
        const promotion = right.value.items[0].key
        right.value.items[0].key = null
        branch.entry.value.items = branch.entry.value.items.splice(0, partition)
        parent.entry.value.items.splice(parent.index + 1, 0, { key: promotion, id: rightId })
        const commit = new Commit(this)
        const prepare = []
        // Write the new branch to a temporary file.
        prepare.push(await commit.emplace(right))
        prepare.push(await commit.emplace(branch.entry))
        prepare.push(await commit.emplace(parent.entry))
        // Record the commit.
        await commit.write(prepare)
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        entries.forEach(entry => entry.release())
        await this._possibleSplit(parent.entry.value, key, parent.level)
        // TODO Is this necessary now that we're splitting a page at a time?
        // await this._possibleSplit(branch.entry.value, key, level)
        // await this._possibleSplit(right.value, partition, level)
    }

    // TODO We need to block writes to the new page as well. Once we go async
    // again, someone could descend the tree and start writing to the new page
    // before we get a chance to write the new page stub.
    //
    // ^^^ Coming back to the project and this was not done. You'd simply
    // calculate the new id before requesting your blocks, request two blocks.

    //
    async _splitLeaf (key, child, entries) {
        // TODO Add right page to block.
        const blocks = []
        const block = this._block(child.entry.value.id)
        await block.enter.promise
        blocks.push(block)

        const parent = await this.descend({ key, level: child.level - 1 }, entries)

        // Race is the wrong word, it's our synchronous time. We have to split
        // the page and then write them out. Anyone writing to this leaf has to
        // to be able to see the split so that they surrender their cursor if
        // their insert or delete belongs in the new page, not the old one.
        //
        // Notice that all the page manipulation takes place before the first
        // write. Recall that the page manipulation is done to the page in
        // memory which is offical, the page writes are lagging.

        // Split page creating a right page.
        const left = child.entry.value
        const length = child.entry.value.items.length
        const partition = Math.floor(length / 2)
        const items = child.entry.value.items.splice(partition)
        const heft = items.reduce((sum, item) => sum + item.heft, 0)
        const right = this._create({
            id: this._nextId(true),
            leaf: true,
            items: items,
            entries: [],
            right: child.entry.value.right,
            append: this._filename(),
            ghosts: 0
        })
        blocks.push(this._block(right.value.id))
        entries.push(right)
        right.heft = heft

        // Set the right key of the left page.
        child.entry.value.right = right.value.items[0].key

        // Set the heft of the left page and entry.
        child.entry.heft -= heft

        // Insert a reference to the right page in the parent branch page.
        parent.entry.value.items.splice(parent.index + 1, 0, {
            key: right.value.items[0].key,
            id: right.value.id,
            heft: 0
        })

        // If any of the pages is still larger than the split threshhold, check
        // the split again.
        for (const page of [ right.value, child.entry.value ]) {
            if (page.items.length >= this.leaf.split) {
                this._housekeeping.add(page.items[0].key)
            }
        }


        // Write any queued writes, they would have been in memory, in the page
        // that was split above. Once we await, items can be inserted or removed
        // from the page in memory. Our synchronous operations are over.
        const append = this._filename()
        const dependents = [{
            header: { method: 'dependent', id: child.entry.value.id, append },
            parts: []
        }, {
            header: { method: 'dependent', id: right.value.id, append: right.value.append },
            parts: []
        }]
        const writes = this._queue(child.entry.value.id).writes.splice(0)
        writes.push.apply(writes, dependents)
        await this._writeLeaf(child.entry.value.id, writes)

        child.entry.value.entries.push.apply(child.entry.value.entries, dependents)

        // Curious race condition here, though, where we've flushed the page to
        // split

        // TODO Make header a nested object.

        // Create our journaled tree alterations.
        const commit = new Commit(this)

        const prepare = []


        // Record the split of the right page in a new stub.
        prepare.push({
            method: 'stub',
            page: { id: right.value.id, append: right.value.append },
            records: [{
                method: 'load',
                id: child.entry.value.id,
                append: child.entry.value.append
            }, {
                method: 'slice',
                index: partition,
                length: length,
            }]
        })
        right.value.entries = [{
            header: { method: 'load', id: child.entry.value.id, append: child.entry.value.append },
            entries: child.entry.value.entries
        }]

        // Record the split of the left page in a new stub, for which we create
        // a new append file.
        prepare.push({
            method: 'stub',
            page: { id: child.entry.value.id, append },
            records: [{
                method: 'load',
                id: child.entry.value.id,
                append: child.entry.value.append
            }, {
                method: 'slice',
                index: 0,
                length: partition
            }]
        })
        child.entry.value.entries = [{
            header: { method: 'load', id: child.entry.value.id, append: child.entry.value.append },
            entries: child.entry.value.entries
        }]
        child.entry.value.append = append

        // Commit the stubs before we commit the updated branch.
        prepare.push({ method: 'commit' })

        // Write the new branch to a temporary file.
        prepare.push(await commit.emplace(parent.entry))

        // Record the commit.
        await commit.write(prepare)

        // Pretty sure that the separate prepare and commit are merely because
        // we want to release the lock on the leaf as soon as possible.
        await commit.prepare()
        await commit.commit()
        blocks.map(block => block.exit.resolve())
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        // We can release and then perform the split because we're the only one
        // that will be changing the tree structure.
        entries.forEach(entry => entry.release())
        await this._possibleSplit(parent.entry.value, key, parent.level)

        await this._vacuum(key)
        await this._vacuum(right.value.items[0].key)
    }

    async _selectMerger (key, child, entries) {
        const level = child.entry.value.leaf ? -1 : child.level
        const left = await this.descend({ key, level, fork: true }, entries)
        const right = child.right == null
                    ? null
                    : await this.descend({ key: child.right, level }, entries)
        const mergers = []
        if (left != null) {
            mergers.push({
                count: left.entry.value.items.length,
                key: child.entry.value.items[0].key,
                level: level
            })
        }
        if (right != null) {
            mergers.push({
                count: right.entry.value.items.length,
                key: child.right,
                level: level
            })
        }
        return mergers.sort((left, right) => left.count - right.count).shift()
    }

    _isDirty (page, sizes) {
        return page.items.length >= sizes.split ||
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
                if (ancestor.entry.value.items.length == 1) {
                    surgery.deletions.push(ancestor)
                } else {
                    // TODO Also null out after splice.
                    assert.equal(ancestor.index, 0, 'unexpected ancestor')
                    surgery.replacement = ancestor.entry.value.items[1].key
                    surgery.splice = ancestor
                }
                level--
            } while (surgery.replacement == null && level != right.pivot.level)
        }

        return surgery
    }

    async _fill (key) {
        const entries = []

        const root = await this.descend({ key, level: 0 }, entries)
        const child = await this.descend({ key, level: 1 }, entries)

        root.entry.value.items = child.entry.value.items
        root.heft = child.heft

        // Create our journaled tree alterations.
        const commit = new Commit(this)
        const prepare = []

        // Write the merged page.
        prepare.push(await commit.emplace(root.entry))

        // Delete the page merged into the merged page.
        prepare.push({
            method: 'unlink',
            path: path.join('pages', child.entry.value.id)
        })

        // Record the commit.
        await commit.write(prepare)

        // Pretty sure that the separate prepare and commit are merely because
        // we want to release the lock on the leaf as soon as possible.
        await commit.prepare()
        await commit.commit()
        await commit.dispose()

        entries.forEach(entry => entry.release())
    }

    async _possibleMerge (surgery, key, branch) {
        if (surgery.splice.entry.value.items.length <= this.branch.merge) {
            if (surgery.splice.entry.value.id != '0.0') {
                // TODO Have `_selectMerger` manage its own entries.
                const entries = []
                const merger = await this._selectMerger(key, surgery.splice, entries)
                entries.forEach(entry => entry.release())
                await this._mergeBranch(merger)
            } else if (branch && this.branch.merge == 1) {
                await this._fill(key)
            }
        }
    }

    async _mergeBranch ({ key, level }) {
        const entries = []

        const left = await this.descend({ key, level, fork: true }, entries)
        const right = await this.descend({ key, level }, entries)

        const pivot = await this.descend(right.pivot, entries)

        const surgery = await this._surgery(right, pivot)

        right.entry.value.items[0].key = key
        left.entry.value.items.push.apply(left.entry.value.items, right.entry.value.items)

        // Replace the key of the pivot if necessary.
        if (surgery.replacement != null) {
            pivot.entry.value.items[pivot.index].key = surgery.replacement
        }

        // Remove the branch page that references the leaf page.
        surgery.splice.entry.value.items.splice(surgery.splice.index, 1)

        // If the splice index was zero, null the key of the new left most branch.
        if (surgery.splice.index == 0) {
            surgery.splice.entry.value.items[0].key = null
        }

        // Heft will be adjusted by serialization, but let's do this for now.
        left.entry.heft += right.entry.heft

        // Create our journaled tree alterations.
        const commit = new Commit(this)

        const prepare = []

        // Write the merged page.
        prepare.push(await commit.emplace(left.entry))

        // Delete the page merged into the merged page.
        prepare.push({
            method: 'unlink',
            path: path.join('pages', right.entry.value.id)
        })

        // If we replaced the key in the pivot, write the pivot.
        if (surgery.replacement != null) {
            prepare.push(await commit.emplace(pivot.entry))
        }

        // Write the page we spliced.
        prepare.push(await commit.emplace(surgery.splice.entry))

        // Delete any removed branches.
        for (const deletion in surgery.deletions) {
            prepare.push({
                method: 'unlink',
                path: path.join('pages', deletion.entry.value.id)
            })
        }

        // Record the commit.
        await commit.write(prepare)

        // Pretty sure that the separate prepare and commit are merely because
        // we want to release the lock on the leaf as soon as possible.
        await commit.prepare()
        await commit.commit()
        await commit.dispose()

        let leaf = left.entry
        // We don't have to restart our descent on a cache miss because we're
        // the only ones altering the shape of the tree.
        //
        // TODO I'm sure there is a way we can find this on a descent somewhere,
        // that way we don't have to test this hard-to-test cache miss.
        while (!leaf.value.leaf) {
            const id = leaf.value.items[0].id
            leaf = this._hold(id)
            if (leaf.value == null) {
                leaf.remove()
                entries.push(leaf = await this.load(id))
            } else {
                entries.push(leaf)
            }
        }

        entries.forEach(entry => entry.release())

        await this._possibleMerge(surgery, leaf.value.items[0].key, true)
    }

    async _mergeLeaf ({ key, level }) {
        const entries = []

        const left = await this.descend({ key, level, fork: true }, entries)
        const right = await this.descend({ key, level }, entries)

        const pivot = await this.descend(right.pivot, entries)

        const surgery = await this._surgery(right, pivot)

        const blocks = [
            this._block(left.entry.value.id),
            this._block(right.entry.value.id)
        ]

        // Block writes to both pages.
        for (const block of blocks) {
            await block.enter.promise
        }

        // Add the items in the right page to the end of the left page.
        const items = left.entry.value.items
        items.push.apply(items, right.entry.value.items.splice(right.entry.value.ghosts))

        // Set right reference of left page.
        left.entry.value.right = right.entry.value.right

        // Adjust heft of left entry.
        left.entry.heft += right.entry.heft

        // Mark the right page deleted, it will cause `indexOf` in the `Cursor`
        // to return `null` indicating that the user must release the `Cursor`
        // and descend again.
        right.entry.value.deleted = true

        // See if the merged page needs to split or merge further.
        if (this._isDirty(left.entry.value, this.leaf)) {
            this._housekeeping.push(left.entry.value.items[0].key)
        }

        // Replace the key of the pivot if necessary.
        if (surgery.replacement != null) {
            pivot.entry.value.items[pivot.index].key = surgery.replacement
        }

        // Remove the branch page that references the leaf page.
        surgery.splice.entry.value.items.splice(surgery.splice.index, 1)

        if (surgery.splice.index == 0) {
            surgery.splice.entry.value.items[0].key = null
        }

        // Now we've rewritten the branch tree and merged the leaves. When we go
        // asynchronous `Cursor`s will be invalid and they'll have to descend
        // again. User writes will continue in memory, but leaf page writes are
        // currently blocked. We start by flushing any cached writes.
        //
        // TODO Apparently we don't add a dependent record to the left since it
        // has the same id, we'd depend on ourselves, but vacuum ought to erase
        // it.
        const writes = {
            left: this._queue(left.entry.value.id).writes.splice(0),
            right: this._queue(right.entry.value.id).writes.splice(0).concat({
                header: {
                    method: 'dependent',
                    id: left.entry.value.id,
                    append: left.entry.value.append
                },
                parts: []
            })
        }

        await this._writeLeaf(left.entry.value.id, writes.left)
        await this._writeLeaf(right.entry.value.id, writes.right)

        // Create our journaled tree alterations.
        const commit = new Commit(this)

        const prepare = []

        // Record the split of the right page in a new stub.
        const append = this._filename()
        prepare.push({
            method: 'stub',
            page: { id: left.entry.value.id, append: append },
            records: [{
                method: 'load',
                id: left.entry.value.id,
                append: left.entry.value.append
            }, {
                method: 'merge',
                id: right.entry.value.id,
                append: right.entry.value.append
            }]
        })
        // TODO Okay, forgot what `entries` is and it appears to be just the
        // entries needed to determine dependencies so we can unlink files when
        // we vaccum.
        left.entry.value.entries = [{
            method: 'load', id: left.entry.value.id, append: left.entry.value.append,
            entries: left.entry.value.entries
        }, {
            method: 'merge', id: right.entry.value.id, append: right.entry.value.append,
            entries: right.entry.value.entries
        }]
        left.entry.value.append = append

        // Commit the stub before we commit the updated branch.
        prepare.push({ method: 'commit' })

        // If we replaced the key in the pivot, write the pivot.
        if (surgery.replacement != null) {
            prepare.push(await commit.emplace(pivot.entry))
        }

        // Write the page we spliced.
        prepare.push(await commit.emplace(surgery.splice.entry))

        // Delete any removed branches.
        for (const deletion in surgery.deletions) {
            prepare.push({
                method: 'unlink',
                path: path.join('pages', deletion.entry.value.id)
            })
        }

        // Record the commit.
        await commit.write(prepare)

        // Pretty sure that the separate prepare and commit are merely because
        // we want to release the lock on the leaf as soon as possible.
        await commit.prepare()
        await commit.commit()
        for (const block of blocks) {
            block.exit.resolve()
        }
        await commit.prepare()
        await commit.commit()
        await commit.dispose()

        // We can release and then perform the split because we're the only one
        // that will be changing the tree structure.
        entries.forEach(entry => entry.release())

        await this._possibleMerge(surgery, left.entry.value.items[0].key, false)
    }

    // TODO Must wait for housekeeping to finish before closing.
    async _housekeeper ({ body: key }) {
        const entries = []
        const child = await this.descend({ key }, entries)
        if (child.entry.value.items.length >= this.leaf.split) {
            await this._splitLeaf(key, child, entries)
        } else if (
            ! (
                child.entry.value.id == '0.1' && child.entry.value.right == null
            ) &&
            child.entry.value.items.length <= this.leaf.merge
        ) {
            const merger = await this._selectMerger(key, child, entries)
            entries.forEach(entry => entry.release())
            await this._mergeLeaf(merger)
        } else {
            entries.forEach(entry => entry.release())
        }
    }
}

module.exports = Journalist
