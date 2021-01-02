'use strict'

const path = require('path')
const fs = require('fs').promises
const assert = require('assert')
const Journalist = require('journalist')
const Magazine = require('magazine')

const Sheaf = require('./sheaf')
const Strata = { Error: require('./error') }
const Player = require('transcript/player')
const io = require('./io')

function _path (...vargs) {
    return path.join.apply(path, vargs.map(varg => String(varg)))
}

// Sort function for file names that orders by their creation order.
const appendable = require('./appendable')

class FileSystem {
    constructor (directory, handles) {
        this.directory = directory
        this.handles = handles
        this._id = 0
    }

    create (sheaf) {
        return new FileSystem.Writer(this, sheaf)
    }
    //

    // **TODO** Really need to think of some rules for failure. They may be
    // interim rules, like alpha release rules. For now we ask that you let
    // stuff crash, provide as much detail as Strata will provide, we
    // probably can't fix anything other than to try to make those crash
    // reports better for the future.

    // `libuv` suppresses `EPROGRESS` and `EINTR` errors treating them as
    // successful, so the only remaining error would be `EIO` which probably
    // means that the file is corrupt.

    // We want this class to be independent of an particular strata so it
    // can be shared.

    //
    static HandleCache = class extends Magazine.OpenClose {
        constructor (magazine, strategy = 'O_SYNC') {
            super(magazine)
            this.strategy = strategy
        }
        subordinate () {
            return this._subordinate(new HandleCache(this._sync))
        }
        async open (filename) {
            const flag = this.strategy == 'O_SYNC' ? 'as' : 'a'
            return await Strata.Error.resolve(fs.open(filename, flag), 'IO_ERROR')
        }
        async close (handle) {
            if (this.strategy == 'fsync') {
                await Strata.Error.resolve(handle.sync(), 'IO_ERROR')
            }
            await Strata.Error.resolve(handle.close(), 'IO_ERROR')
        }
    }

    static Reader = class {
        constructor (directory, options = {}) {
            options = Sheaf.options(options)
            this.directory = directory
            this.serializer = options.serializer
            this.extractor = options.extractor
            this.checksum = options.checksum
        }

        _path (...vargs) {
            vargs.unshift(this.directory)
            return path.resolve.apply(path, vargs.map(varg => String(varg)))
        }

        async _appendable (id) {
            const dir = await fs.readdir(this._path('pages', id))
            return dir.filter(file => /^\d+\.\d+$/.test(file)).sort(appendable).pop()
        }

        async log (id, log) {
            if (log == null) {
                log = await this._appendable(id)
            }
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
                            const { page: previous } = await this.log(header.page, header.log)
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
                            const { page: right } = await this.log(header.page, header.log)
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
                    }
                }
            }
            state.heft = page.items.reduce((sum, record) => sum + record.heft, 1)
            return { page, ...state }
        }

        async page (id) {
            const leaf = +id.split('.')[1] % 2 == 1
            if (leaf) {
                const { page, heft } = await this.log(id, null)
                assert(page.id == '0.1' ? page.key == null : page.key != null)
                return { page, heft }
            }
            const player = new Player(function () { return '0' })
            const items = []
            const buffer = Buffer.alloc(1024 * 1024)
            // **TODO** Length was there so that a branch page could be
            // verified, if it was truncated. Let's turn this into a log and
            // play entries instead of having a different type, so it ends up
            // looking like writeahead page. We can then push the length instead
            // of prepending it.
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
    }

    static Writer = class {
        constructor (fileSystem, sheaf) {
            this.directory = fileSystem.directory
            this.handles = fileSystem.handles
            this.sheaf = sheaf
            this.instance = 0
            this._id = 0
            this.reader = new FileSystem.Reader(this.directory, sheaf.options)
        }

        nextId (leaf) {
            let id
            do {
                id = this._id++
            } while (leaf ? id % 2 == 0 : id % 2 == 1)
            return String(this.instance) + '.' +  String(id)
        }

        _path (...vargs) {
            vargs.unshift(this.directory)
            return path.resolve.apply(path, vargs.map(varg => String(varg)))
        }

        _filename (id) {
            return `${this.instance}.${this._id++}`
        }

        _recordify (header, parts = []) {
            return this.sheaf._recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
        }

        async create () {
            const directory = this.directory
            const stat = await Strata.Error.resolve(fs.stat(directory), 'IO_ERROR')
            Strata.Error.assert(stat.isDirectory(), 'CREATE_NOT_DIRECTORY', { directory })
            const dir = await Strata.Error.resolve(fs.readdir(directory), 'IO_ERROR')
            Strata.Error.assert(dir.every(file => /^\./.test(file)), 'CREATE_NOT_EMPTY', { directory })
            await Strata.Error.resolve(fs.mkdir(this._path('instances')), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('pages')), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance')), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('instances', '0'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('page'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance', '0.0'), { recursive: true }), 'IO_ERROR')
            await Strata.Error.resolve(fs.mkdir(this._path('balance', '0.1')), 'IO_ERROR')
            const buffers = [ this._recordify({ length: 1 }), this._recordify({ id: '0.1' }, []) ]
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.0', 'page'), Buffer.concat(buffers), { flag: 'as' }), 'IO_ERROR')
            const zero = this._recordify({ method: '0.0' })
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.1', '0.0'), zero, { flag: 'as' }), 'IO_ERROR')
            const one = this._recordify({ method: 'load', page: '0.1', log: '0.0' })
            await Strata.Error.resolve(fs.writeFile(this._path('balance', '0.1', '0.1'), one, { flag: 'as' }), 'IO_ERROR')
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
        }

        async open () {
            // **TODO** Run commit log on reopen.
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
        }

        read (id) {
            return this.reader.page(id)
        }

        async writeLeaf (page, writes) {
            if (writes.length != 0) {
                const filename = this._path('pages', page.id, page.log.id)
                const cartridge = await this.handles.get(filename)
                try {
                    await io.writev(cartridge.value, writes)
                } finally {
                    cartridge.release()
                }
            }
        }

        async _stub (journalist, { log: { id, page }, entries }) {
            const buffers = entries.map(entry => this._recordify(entry.header, entry.parts))
            await Strata.Error.resolve(fs.mkdir(this._path('balance', page), { recursive: true }), 'IO_ERROR')
            const filename = this._path('balance', page, id)
            await io.write(filename, buffers, this.handles.strategy)
            journalist.rename(_path('balance', page, id), _path('pages', page, id))
        }

        async _writeBranch (journalist, branch, create) {
            const filename = this._path('balance', branch.page.id, 'page')
            await Strata.Error.resolve(fs.mkdir(path.dirname(filename), { recursive: true }), 'IO_ERROR')
            const buffers = branch.page.items.map((item, index) => {
                const { id, key } = item
                const parts = key != null ? this.sheaf.serializer.key.serialize(key) : []
                return this._recordify({ id }, parts)
            })
            branch.cartridge.heft = buffers.reduce((sum, buffer) => sum + buffer.length, 0)
            buffers.unshift(this._recordify({ length: branch.page.items.length }))
            await io.write(filename, buffers, this.handles.strategy)
            if (create) {
                journalist.mkdir(_path('pages', branch.page.id))
            } else {
                journalist.unlink(_path('pages', branch.page.id, 'page'))
            }
            journalist.rename(_path('balance', branch.page.id, 'page'), _path('pages', branch.page.id, 'page'))
            journalist.rmdir(_path('balance', branch.page.id))
        }

        _unlink (loaded, page) {
            for (const log of loaded) {
                if (log.page == page) {
                    this.journalist.unlink(_path('pages', log.page, log.id))
                    this._unlink(log.loaded)
                }
            }
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
        async _vacuum (key, cartridges) {
            //

            // Obtain the pages so that they are not read while we are rewriting
            // their log history.

            //
            const loaded = (await this.sheaf.descend({ key }, cartridges)).page.log.loaded
            //

            // We want the log history of the page.

            //
            Strata.Error.assert(loaded.length == 1 && loaded[0].replaceable, 'VACUUM_PREVIOUS_NOT_REPLACABLE')
            const log = loaded[0]
            //

            // We don't use the cached page. We read the log starting from the
            // replacable log entry.

            //
            const { page: page, split } = await this.reader.log(log.page, log.id)
            Strata.Error.assert(page.log.replaceable, 'STRANGE_VACUUM_STATE')
            Strata.Error.assert(page.id == log.page, 'STRANGE_VACUUM_STATE')
            if (split != null) {
                const { page: dependent } = await this.reader.page(split)
                Strata.Error.assert(dependent.log.loaded.length == 1 && ! dependent.log.loaded[0].replaceable, 'UNVACUUMED_DEPENDENT')
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
            const filename = this._path('balance', page.id, page.log.id)
            const buffers = page.items.map((item, index) => {
                const parts = this.sheaf.serializer.parts.serialize(item.parts)
                return this._recordify({ method: 'insert', index }, parts)
            })
            await io.write(filename, buffers, this.handles.strategy)

            /*
            const buffers = page.items.map((item, index) => {
                const parts = this.sheaf.serializer.parts.serialize(item.parts)
                return this._recordify({ method: 'insert', index }, parts)
            })

            await Strata.Error.resolve(fs.writeFile(this._path('balance', page.id, page.log.id), Buffer.concat(buffers), { flag: 'as' }), 'IO_ERROR')
            */

            this.journalist.unlink(_path('pages', page.id, page.log.id))
            this.journalist.rename(_path('balance', page.id, page.log.id), _path('pages', page.id, page.log.id))
            this.journalist.rmdir(_path('balance', page.id))

            this._unlink(page.log.loaded, page.id)

            loaded[0].loaded.length = 0
            loaded[0].replaceable = false
        }

        async writeDrainRoot ({ left, right, root }) {
            await this._writeBranch(this.journalist, right, true)
            await this._writeBranch(this.journalist, left, true)
            await this._writeBranch(this.journalist, root, false)
        }

        async writeSplitBranch ({ left, right, parent }) {
            await this._writeBranch(this.journalist, left, false)
            await this._writeBranch(this.journalist, right, true)
            await this._writeBranch(this.journalist, parent, false)
        }

        async writeSplitLeaf ({ key, left, right, parent, writes, messages }) {
            const journalist = await Journalist.create(this.directory)

            const partition = left.page.items.length
            const length = left.page.items.length + right.page.items.length

            await this.writeLeaf(left.page, writes)
            //

            // Create the new page directory in our journal.

            //
            journalist.mkdir(_path('pages', right.page.id))
            //

            // Pages are broken up into logs. The logs have a load instruction
            // that will tell them to load a previous log, essentially a linked
            // list. They have a split instruction that will tell them to split the
            // page they loaded. When reading it will load the new head which will
            // tell it to load the previous page and split it.

            // Except we don't want to have an indefinate linked list. We vacuum
            // when we split. We do this by inserting a place holder log between the
            // old log and the new log. The place holder conatains just the load and
            // split operation. After these two small files are written and synced,
            // we can release our pause on writes on the cache page and move onto
            // vacuum.

            // New writes will go to the head of the log. We will replace our
            // place-holder with a vacuumed copy of the previous log each page
            // receiving just its half of the page will all delete operations
            // removed. When we vacuum we only need to hold a cache reference to the
            // page so it will not be evicted and re-read while we're moving the old
            // logs around, so vacuuming can take place in parallel to all user
            // operations.
            //
            // The replacement log will also include an indication of dependency. It
            // will mark a `split` property in the page for the left page. During
            // vacuum the we will check the `split` property of the page created by
            // reading the replacable part of the log. If it is not null we will
            // assert that the dependent page is vacuumed exist before we vacuum.
            // This means we must vacuum the right page befroe we vacuum the left
            // page.

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
                            length: left.page.items.length,
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
                        parts: this.sheaf.serializer.key.serialize(right.page.key)
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
                        parts: this.sheaf.serializer.key.serialize(right.page.key)
                    }]
                }
            }

            if (left.page.id != '0.1') {
                stub.left.entries.push({
                    header: { method: 'key' },
                    parts: this.sheaf.serializer.key.serialize(left.page.key)
                })
            }

            if (right.page.right != null) {
                stub.right.entries.push({
                    header: { method: 'right' },
                    parts: this.sheaf.serializer.key.serialize(right.page.right)
                })
            }

            await this._stub(journalist, stub.left)
            await this._stub(journalist, stub.right)
            //

            // Update the log history.

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

            // Here we add messages to our journal saying what we want to do next.
            // We run a journal for each step.

            //
            messages.push({ method: 'vacuum', key: key })
            messages.push({ method: 'vacuum', key: right.page.key })

            messages.forEach(message => journalist.message(message))

            // Run the journal, prepare it and commit it. If prepare fails the split
            // never happened, we'll split the page the next time we visit it. If
            // commit fails everything we did above will happen in recovery.

            //
            await journalist.prepare()
            await journalist.commit()
        }

        async writeFillRoot({ root, child }) {
            await this._writeBranch(this.journalist, root, false)

            this.journalist.unlink(_path('pages', child.page.id, 'page'))
            this.journalist.rmdir(_path('pages', child.page.id))
        }

        async writeMergeBranch({ key, left, right, pivot, surgery }) {
            // Write the merged page.
            await this._writeBranch(this.journalist, left, false)

            // Delete the page merged into the merged page.
            this.journalist.unlink(_path('pages', right.page.id, 'page'))
            this.journalist.rmdir(_path('pages', right.page.id))

            // If we replaced the key in the pivot, write the pivot.
            if (surgery.replacement != null) {
                await this._writeBranch(this.journalist, pivot, false)
            }

            // Write the page we spliced.
            await this._writeBranch(this.journalist, surgery.splice, false)

            // Delete any removed branches.
            for (const deletion in surgery.deletions) {
                throw new Error
                await commit.unlink(path.join('pages', deletion.entry.value.id))
            }
        }

        async _rmrf (pages) {
            for (const id of pages) {
                const leaf = +id.split('.')[1] % 2 == 1
                if (leaf) {
                    const { page, merged } = await this.reader.log(id)
                    Strata.Error.assert(merged != null, 'DELETING_UNMERGED_PAGE')
                    await fs.rmdir(this._path('pages', id), { recursive: true })
                } else {
                    await fs.rmdir(this._path('pages', id), { recursive: true })
                }
            }
        }

        async writeMergeLeaf({ key, left, right, surgery, pivot, writes, messages }) {
            const journalist = await Journalist.create(this.directory)

            await this.writeLeaf(left.page, writes.left)
            await this.writeLeaf(right.page, writes.right)
            //

            // We discuss this in detail in `_splitLeaf`. We want a record of
            // dependents and we probably want that to be in the page directory of
            // each page if we're going to do some sort of audit that includes a
            // directory scan looking for orphans.

            // We know that the left page into which we merged already has a
            // dependent record so we need to add one...

            // Maybe we do not have a dependent record that references the self,
            // only the other. This makes more sense. It would be easier to test
            // that dependents are zero. There is only ever one dependent record and
            // if it the same page as the loaded page it is a merge, otherwise it is
            // a split.

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
                    parts: this.sheaf.serializer.key.serialize(right.page.right)
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
            //
            messages.push({
                method: 'rmrf',
                pages: surgery.deletions.map(deletion => deletion.page.id).concat(right.page.id)
            }, {
                method: 'vacuum', keys: [ key ]
            })

            messages.forEach(message => journalist.message(message))
            //

            // Delete our scrap directories.

            //
            journalist.rmdir(_path('balance', left.page.id))
            journalist.rmdir(_path('balance', right.page.id))
            //

            // Record the commit.
            await journalist.prepare()
            await journalist.commit()
        }

        async balance () {
            for (;;) {
                this.journalist = await Journalist.create(this.directory)
                if (this.journalist.messages.length == 0) {
                    await this.journalist.dispose()
                    this.journalist = null
                    break
                }
                const messages = this.journalist.messages
                const message = messages.pop()
                const cartridges = []
                switch (message.method) {
                case 'vacuum':
                    await this._vacuum(message.key, cartridges)
                    break
                case 'rmrf':
                    await this._rmrf(message.pages)
                    break
                case 'balance':
                    await this.sheaf.balance(message.key, message.level, messages, cartridges)
                    break
                }
                messages.forEach(message => this.journalist.message(message))
                await this.journalist.prepare()
                await this.journalist.commit()
                cartridges.forEach(cartridge => cartridge.release())
            }
        }
    }
}

module.exports = FileSystem
