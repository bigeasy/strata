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
const coalesece = require('extant')
const Future = require('prospective/future')
const Commit = require('./commit')
const fnv = require('./fnv')

const Turnstile = require('turnstile')
Turnstile.Queue = require('turnstile/queue')

const Fracture = require('fracture')

function traceIf (condition) {
    if (condition) return function (...vargs) {
        console.log.apply(console, vargs)
    }
    return function () {}
}

const appendable = require('./appendable')

const Strata = { Error: require('./error') }

function increment (value) {
    return value + 1 & 0xffffffff
}

class Journalist {
    constructor (destructible, options) {
        const leaf = coalesece(options.leaf, {})
        this.leaf = {
            split: coalesece(leaf.split, 5),
            merge: coalesece(leaf.merge, 2)
        }
        const branch = coalesece(options.branch, {})
        this.branch = {
            split: coalesece(branch.split, 5),
            merge: coalesece(branch.merge, 2)
        }
        this.cache = options.cache
        this.instance = 0
        this.directory = options.directory
        this.comparator = options.comparator || ascension([ String ], (value) => [ value ])
        this._recorder = recorder(() => '0')
        this._root = null
        this._operationId = 0xffffffff
        const appending = new Fracture(destructible.durable('appender'))
        this._appending = new Turnstile.Queue(appending, this._append, this)
        this._queues = {}
        this._blockId = 0xffffffff
        this._blocks = [{}]
        const housekeeping = new Turnstile(destructible.durable('housekeeper'))
        this._housekeeping = new Turnstile.Queue(housekeeping, this._housekeeper, this)
        this._dirty = {}
        this._id = 0
        this.closed = false
        this.destroyed = false
        destructible.destruct(() => this.destroyed = true)
    }

    async create () {
        const directory = this.directory
        this._root = this.cache.hold([ directory, -1 ], { items: [{ id: '0.0' }] })
        const stat = await fs.stat(directory)
        Strata.Error.assert(stat.isDirectory(), 'create.not.directory', { directory: directory })
        Strata.Error.assert((await fs.readdir(directory)).filter(file => {
            return ! /^\./.test(file)
        }).length == 0, 'create.directory.not.empty', { directory: directory })
        await fs.mkdir(path.resolve(directory, 'instance', '0'), { recursive: true })
        const pages = path.resolve(directory, 'pages')
        await fs.mkdir(path.resolve(pages, '0.0'), { recursive: true })
        const buffer = Buffer.from(JSON.stringify([{ id: '0.1', key: null }]))
        const hash = fnv(buffer)
        await fs.writeFile(path.resolve(pages, '0.0', hash), buffer)
        await fs.mkdir(path.resolve(pages, '0.1'), { recursive: true })
        await fs.writeFile(path.resolve(pages, '0.1', '0.0'), Buffer.alloc(0))
    }

    async open () {
        const directory = this.directory
        this._root = this.cache.hold([ directory, -1 ], { items: [{ id: '0.0' }] })
        const instances = (await fs.readdir(path.join(directory, 'instances')))
            .filter(file => /^\d+$/.test(file))
            .map(file => +file)
            .sort((left, right) => right - left)
        this.instance = instances[0] + 1
        await fs.mkdir(path.join(directory, 'instances', String(this.instance)))
        for (let instance of instances) {
            await fs.rmdir(path.resolve(directory, 'instances', String(instance)))
        }
    }

    async _hashable (id) {
        const regex = /^[a-z0-9]+$/
        const dir = await fs.readdir(path.join(this.directory, 'pages', id))
        const files = dir.filter(file => regex.test(file))
        assert.equal(files.length, 1, `multiple branch page files: ${id}, ${files}`)
        return files.pop()
    }

    async _appendable (id) {
        const dir = await fs.readdir(path.join(this.directory, 'pages', id))
        return dir.filter(file => /^\d+\.\d+$/.test(file)).sort(appendable).pop()
    }

    async _read (id, append) {
        const page = { id, leaf: true, items: [], right: null, ghosts: 0, append }
        const player = new Player(function () { return '0' })
        const directory = path.resolve(this.directory, 'pages', String(id))
        const filename = path.join(directory, append)
        const readable = fileSystem.createReadStream(filename)
        for await (let chunk of readable) {
            for (let entry of player.split(chunk)) {
                switch (entry.header.method) {
                case 'right': {
                        page.right = entry.header.right
                    }
                    break
                case 'load': {
                        const { page: loaded } = await this._read(entry.header.id, entry.header.append)
                        page.items = loaded.items
                        page.right = loaded.right
                    }
                    break
                case 'slice': {
                        if (entry.header.length < page.items.length) {
                            page.right = page.items[entry.header.length].key
                        }
                        page.items = page.items.slice(entry.header.index, entry.header.length)
                    }
                    break
                case 'insert': {
                        page.items.splice(entry.header.index, 0, {
                            key: entry.header.key,
                            value: entry.body,
                            heft: entry.sizes[0] + entry.sizes[1]
                        })
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
        const entry = this._hold(id, null)
        if (entry.value == null) {
            const { page, heft } = await this.read(id)
            entry.value = page
            entry.heft = heft
        }
        return entry
    }

    _hold (id, initial) {
        return this.cache.hold([ this.directory, id ], initial)
    }

    // TODO If `key` is `null` then just go left.
    _descend (entries, { key, level = -1, fork = 0 }) {
        const descent = { miss: null, keyed: null, level: 0, index: 0, entry: null }
        let entry = null
        entries.push(entry = this._hold(-1, null))
        for (;;) {
            // You'll struggle to remember this, but it is true...
            if (descent.index != 0) {
                // The last key we visit is the key for the leaf page, if we're
                // headed to a leaf. We don't have to have the exact leaf key,
                // so if housekeeping is queued up in such a way that a leaf
                // page in the queue is absorbed by a merge prior to its
                // housekeeping inspection, the descent on that key is not going
                // to cause a ruckus. Keys are not going to disappear on us when
                // we're doing branch housekeeping.
                descent.keyed = {
                    key: entry.value.items[descent.index].key,
                    level: descent.level
                }
                // If we're trying to find siblings we're using an exact key
                // that is definately above the level sought, we'll see it and
                // then go left or right if there is a branch in that direction.
                //
                // TODO Earlier I had this at KILLROY below. And I adjust the
                // level, but I don't reference the level, so it's probably fine
                // here.
                if (descent.keyed.key == key) {
                    if (fork) {
                        if (descent.index == 0) {
                            return null
                        }
                        // Go right down to the desired level.
                        throw new Error
                    }
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
            entries.push(entry = this._hold(id, null))
            if (entry.value == null) {
                entries.pop().remove()
                return { miss: id }
            }

            // Binary search the page for the key.
            const offset = entry.value.leaf ? entry.value.ghosts : 1
            const index = find(this.comparator, entry.value, key, offset)

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
        return descent
    }

    // We hold onto the entries array for the descent to prevent the unlikely
    // race condition where we cannot descend because we have to load a page,
    // but while we're loading a page another page in the descent unloads.
    //
    // Conceivably, this could continue indefinitely.

    //
    async descend (query) {
        const entries = [[]]
        for (;;) {
            entries.push([])
            const descent = this._descend(entries[1], query)
            entries.shift().forEach((entry) => entry.release())
            if (descent.miss == null) {
                descent.entry = entries[0].pop()
                entries.shift().forEach((entry) => entry.release())
                return descent
            }
            (await this.load(descent.miss)).release()
        }
    }

    async close () {
        if (!this.closed) {
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
        const entry = this._hold(id, null)
        const buffers = writes.map(write => {
            const buffer = recorder(write.header, write.body)
            if (write.header.method == 'insert') {
                entry.heft += (write.record.heft = buffer.length)
            }
            return buffer
        })
        entry.release()
        const file = path.resolve(this.directory, 'pages', id, append)
        await fs.appendFile(file, Buffer.concat(buffers))
    }

    _queue (id) {
        let queue = this._queues[id]
        if (queue == null) {
            queue = this._queues[id] = {
                id: this._operationId = increment(this._operationId),
                writes: [],
                entry: this._hold(id, null),
                promise: this._appending.enqueue({ method: 'write', id }, this._index(id))
            }
        }
        return queue
    }

    // We prevent deadlock on the hash during merge by returning the same block
    // object if it has already been obtained for an existing page.

    //
    _block (blockId, id) {
        const index = this._index(id)
        let block = this._blocks[index][blockId]
        if (block == null) {
            this._blocks[index][blockId] = block = { enter: new Future, exit: new Future }
            this._appending.push({ method: 'block', index, blockId }, index)
        }
        return block
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
    async _append ({ body }) {
        await callback((callback) => process.nextTick(callback))
        const { method } = body
        switch (method) {
        case 'write':
            const { id } = body
            const queue = this._queues[id]
            delete this._queues[id]
            const entry = queue.entry, page = entry.value
            if (
                page.items.length >= this.leaf.split ||
                (
                    (page.id != '0.1' || page.right != null) &&
                    page.items.length <= this.leaf.merge
                )
            ) {
                this._tidy(page.items[0].key)
            }
            await this._writeLeaf(id, queue.writes)
            break
        case 'block':
            const { index, blockId } = body
            const block = this._blocks[index][blockId]
            delete this._blocks[index][blockId]
            block.enter.resolve()
            await block.exit.promise
            break
        }
    }

    _index (id) {
        return id.split('.').reduce((sum, value) => sum + +value, 0) % this._appending.turnstile.health.turnstiles
    }

    append (entry, promises) {
        const queue = this._queue(entry.id)
        queue.writes.push(entry)
        if (promises[queue.id] == null) {
            promises[queue.id] = queue.promise
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
        const root = await this.descend({ key, level: 0 })
        entries.push(root.entry)
        const partition = Math.floor(root.entry.value.items.length / 2)
        // TODO Print `root.page.items` and see that heft is wrong in the items.
        // Why is it in the items and why is it wrong? Does it matter?
        const leftId = this._nextId(false)
        const left = this._hold(leftId, {
            id: leftId,
            offset: 1,
            items: root.entry.value.items.slice(0, partition),
            hash: null
        })
        entries.push(left)
        const rightId = this._nextId(false)
        const right = this._hold(rightId, {
            id: rightId,
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
        const branch = await this.descend({ key, level })
        entries.push(branch.entry)
        const parent = await this.descend({ key, level: level - 1 })
        entries.push(parent.entry)
        const partition = Math.floor(branch.entry.value.items.length / 2)
        const rightId = this._nextId(false)
        const right = this._hold(rightId, {
            id: rightId,
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

    _descentify (page, appendable = false) {
        const entry = this._hold(page.id, page)
        return { entry, entries: [ entry ], append: null }
    }

    // TODO We need to block writes to the new page as well. Once we go async
    // again, someone could descend the tree and start writing to the new page
    // before we get a chance to write the new page stub.
    //
    // ^^^ Coming back to the project and this was not done. You'd simply
    // calculate the new id before requesting your blocks, request two blocks.

    //
    async _splitLeaf (key, child, parent, entries) {
        // TODO Add right page to block.
        const blockId = this._blockId = increment(this._blockId)
        const block = this._block(blockId, child.entry.value.id)
        await block.enter.promise
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
        const length = left.items.length
        const partition = Math.floor(length / 2)
        const items = left.items.splice(partition)
        const heft = items.reduce((sum, item) => sum + item.heft, 0)
        const right = {
            id: this._nextId(true),
            leaf: true,
            items: items,
            right: left.right,
            heft: heft,
            append: this._filename()
        }

        // Set the right key of the left page.
        left.right = right.items[0].key

        // Set the heft of the left page and entry.
        child.entry.heft = (left.heft -= heft)

        // Create an entry for the right page.
        const entry = this._hold(right.id, right)
        entries.push(entry)
        entry.heft = right.heft

        // Insert a reference to the right page in the parent branch page.
        parent.entry.value.items.splice(parent.index + 1, 0, {
            key: right.items[0].key,
            id: right.id,
            heft: 0
        })

        // If any of the pages is still larger than the split threshhold, check
        // the split again.
        for (const page of [ right, left ]) {
            if (page.items.length >= this.leaf.split) {
                this._housekeeping.push(page.items[0].key)
            }
        }

        // Write any queued writes, they would have been in memory, in the page
        // that was split above. Once we await, items can be inserte or removed
        // from the page in memory. Our synchronous operations are over.
        const writes = this._queue(left.id).writes.splice(0)
        await this._writeLeaf(left.id, writes)

        // TODO Make header a nested object.

        // Create our journaled tree alterations.
        const commit = new Commit(this)

        const prepare = []

        // Record the split of the right page in a new stub.
        prepare.push({
            method: 'stub',
            page: { id: right.id, append: right.append },
            records: [{
                method: 'load',
                id: left.id,
                append: left.append
            }, {
                method: 'slice',
                index: partition,
                length: length,
            }]
        })

        // Record the split of the left page in a new stub, for which we create
        // a new append file.
        const append = this._filename()
        prepare.push({
            method: 'stub',
            page: { id: left.id, append },
            records: [{
                method: 'load',
                id: left.id,
                append: left.append
            }, {
                method: 'slice',
                index: 0,
                length: partition
            }]
        })
        left.append = append

        // Commit the stubs before we commit the updated branch.
        prepare.push({ method: 'commit' })

        // Write the new branch to a temporary file.
        prepare.push(await commit.emplace(parent.entry))

        // Record the commit.
        await commit.write(prepare)

        // TODO If we where to use `_dirty` we'd find that we where unable to
        // record as dirty up above. Can we track `_dirty` by the append
        // idenifier? Can't we just delete this sooner, why am I deleting it
        // here?
        delete this._dirty[key]

        // Pretty sure that the separate prepare and commit are merely because
        // we want to release the lock on the leaf as soon as possible.
        await commit.prepare()
        await commit.commit()
        block.exit.resolve()
        await commit.prepare()
        await commit.commit()
        await commit.dispose()
        // We can release and then perform the split because we're the only one
        // that will be changing the tree structure.
        entries.forEach(entry => entry.release())
        await this._possibleSplit(parent.entry.value, key, parent.level)
    }

    // TODO Must wait for housekeeping to finish before closing.
    async _housekeeper ({ body: key }) {
        const entries = []
        const child = await this.descend({ key })
        entries.push.apply(entries, child.entries)
        if (child.entry.value.items.length >= this.leaf.split) {
            const parent = await this.descend({ key, level: child.level - 1 })
            entries.push.apply(entries, parent.entries)
            await this._splitLeaf(key, child, parent, entries)
        } else {
            entries.forEach(entry => entry.release())
        }
    }

    _tidy (key) {
        if (this._dirty[key] == null) {
            this._dirty[key] = true
            this._housekeeping.push(key)
        }
    }
}

module.exports = Journalist
