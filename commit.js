const assert = require('assert')
const fs = require('fs').promises
const path = require('path')

const fnv = require('./fnv')
const rimraf = require('rimraf')

const callback = require('prospective/callback')

const Strata = { Error: require('./error') }

class Commit {
    constructor (journalist) {
        this._journalist = journalist
        this._index = 0
        this._commit = path.join(journalist.directory, 'commit')
    }

    async write (commit) {
        const dir = await this._readdir()
        const unemplaced = dir.filter(file => ! /\d+\.\d+-\d+\.\d+\.[0-9a-f]/)
        assert.deepStrictEqual(unemplaced, [], 'commit directory not empty')
        await this._write('commit', commit)
    }

    // Believe we can just write out into the commit directory, we don't need to
    // move a file into the directory. No, we do want to get a good write and
    // only rename is atomic. What if we had a bad write?

    //
    async _prepare (operation) {
        await this._write(String(this._index++), [ operation ])
    }

    // Recall that `fs.writeFile` overwrites without complaint.

    //
    async _write (file, entries) {
        const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
        const write = path.join(this._commit, 'write')
        await fs.writeFile(write, buffer)
        await fs.rename(write, path.join(this._commit, `${file}.${fnv(buffer)}`))
    }

    async _load (file) {
        const buffer = await fs.readFile(path.join(this._commit, file))
        const hash = fnv(buffer)
        assert.equal(hash, file.split('.')[1], 'commit hash failure')
        return buffer.toString().split('\n').filter(line => line != '').map(JSON.parse)
    }

    async _readdir () {
        await fs.mkdir(this._commit, { recursive: true })
        const dir = await fs.readdir(this._commit)
        return dir.filter(file => ! /^\./.test(file))
    }

    _path (file) {
        return path.join(this._commit, file)
    }

    async _unlink (file) {
        await callback(callback => rimraf(file, callback))
    }

    // How is this not a race condition? I see that we're setting the heft if
    // the page is cached in memory, but we're not holding it. If the page is
    // loading before our commit is over, it will have the wrong heft, will it
    // not?
    //
    // But, it doesn't matter all that much, does it? The heft is advisory.
    // Until we commit, some of these pages will not be visited. If we where to
    // hold onto the cache entries and release them on destruction, the heft
    // wouldn't matter until then, because...
    //
    // Oh, wait, I recall that we don't care about adjusting heft if the record
    // does exist because we will hold it in the Journalist during the commit.
    // The remove is for when we're replaying as part of a recovery.
    //
    // Note that I still don't recall what my reasoning was on truncated writes.
    // Assume that I assumed that Strata is a primative and could act as a
    // write-ahead log. Not sure how I go about recovering from a truncation,
    // though. Cross that bridge. It's a primitive.
    //
    // Now I'm not seeing why I'm doing the heft adjustment here. I'm already
    // holding on in the Journalist and adjusting heft there. Repeating the
    // operations here, so why not just have drain run in this commit object?

    // Split, drain and fill are all pretty direct as far as commit operations
    // go. You can give an address, or not in the case of fill and drain, and
    // the operation is straight forward. From the Jouranlist on branch merge,
    // though, we ought to make the decision in there and leave the two pages.
    //
    // Oh, I remember. Branch heft is the buffer length, and we won't know the
    // buffer length until we serialize the record.

    //
    async _emplace (page, hash) {
    }

    // append is the Strata instance plus an ever increasing integer for for
    // that instance joined by a dot.

    // Similarly, page ids are ths Strata instance plus an ever increasing
    // integer for that instance where the integer is the next even integer for
    // branch pages and the next odd integer for leaf pages.
    //
    // In storage, the page is a directory, the append is the file name. In the
    // commit the filename is the page id and apppend id hyphen joined.
    //
    // The commit is prefixed with the word `commit-` and each step is a file
    // that is decimal integer and hexidecimal integer, that is, step number and
    // hash of contents.
    //
    // We check for a commit, if one exists, we write out the steps as step
    // files. The step files are numbered so we work through them in order. The
    // first step deletes the commit so we won't write the steps again. Each
    // step file is read, it's acton performed, the step file is deleted. After
    // we've run all the steps we delete the commit directory recusively, not
    // from a step file but in any case, because we might have emplacement files
    // that where never prepared and committed.
    //
    // We delete the commit directory recursively to get rid of everything, so
    // we have to maybe make it in all the functions as a first step, but that's
    // okay, I'm not going to fuss about it.

    //
    async emplace ({ page, entry = {} }) {
        await fs.mkdir(this._commit, { recursive: true })
        const buffer = Buffer.from(JSON.stringify(page.items))
        const hash = fnv(buffer)
        entry.heft = buffer.length
        const filename = `${page.append}.${hash}`
        await fs.writeFile(path.join(this._commit, `${page.id}-${filename}`), buffer)
        const from = path.join('commit', `${page.id}-${filename}`)
        const to = path.join('pages', page.id, filename)
        return {
            method: 'emplace',
            page: { id: page.id, append: page.append },
            hash
        }
    }

    // Okay. Now I see. I wanted the commit to be light and easy and minimal, so
    // that it could be written quickly and loaded quickly, but that is only
    // necessary for the leaf. We really want a `Prepare` that will write files
    // for branches instead of this thing that duplicates, but now I'm starting
    // to feel better about the duplication.
    //
    // Seems like there should be some sort of prepare builder class, especially
    // given that there is going to be emplacements followed by this prepare
    // call, but I'm content to have that still be an array.

    //
    async prepare () {
        const dir = await this._readdir()
        const commit = dir.filter(file => /^commit\.[0-9a-f]+$/.test(file)).shift()
        if (commit == null) {
            return false
        }
        const operations = await this._load(commit)
        // Start by deleting the commit script, once this runs we have to move
        // forward through the entire commit.
        await this._prepare([ 'begin' ])
        while (operations.length != 0) {
            const operation = operations.shift()
            assert(!Array.isArray(operation))
            switch (operation.method) {
            // This is the next commit in a series of commits, we write out the
            // remaining operations into a new commit.
            case 'commit': {
                    const entries = operations.splice(0)
                    const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
                    const hash = fnv(buffer)
                    await fs.writeFile(path.join(this._commit, '_commit'), buffer)
                    const from = path.join('commit', '_commit')
                    const to = path.join('commit', `commit.${hash}`)
                    await this._prepare([ 'rename', from, to, hash ])
                }
                break
            // Write out a stub leaf page that splits, merges or vacuums (simply
            // loads) a previous page.
            case 'stub': {
                    const recorder = this._journalist._recorder
                    const buffer = Buffer.concat(operation.records.map(record => recorder(record)))
                    const hash = fnv(buffer)
                    const filename = `${operation.page.id}-${operation.page.append}`
                    const from = path.join('commit', filename)
                    const to = path.join('pages', operation.page.id, operation.page.append)
                    await fs.writeFile(this._path(filename), buffer)
                    await this._prepare([ 'rename', from, to, hash ])
                }
                break
            case 'emplace': {
                    const { page, hash } = operation
                    const unlink = path.join('pages', page.id, `${page.append}.${page.hash}`)
                    const filename = `${page.append}.${hash}`
                    const from = path.join('commit', `${page.id}-${filename}`)
                    const to = path.join('pages', page.id, filename)
                    await this._prepare([ 'rename', from, to, hash ])
                    await this._prepare([ 'unlink', unlink ])
                }
                break
            case 'split': {
                    // Appears that I'm writing out the page items in their
                    // entirety when I know the keys are extracted from the
                    // records, or at least they where once upon a time. Nope.
                    // Looks like that changed. Keys are now explicit.
                    //
                    // Well that simplifies all these operations, doesn't it.
                    //
                    // No, I'm not really doing leaf splits yet. They are based
                    // on a previous page. If I recall, the plan for vacuum is
                    // based on stubs as a linked list. Suppose you can put down
                    // a stub, the rewrite what it was based on, so all pages
                    // after the first split become linked lists.
                    const page = await this._journalist.read(operation[1][0])
                    page.append = operation[1][1]
                    const right = {
                        id: operation[2][0],
                        items: page.items.splice(operation[3]),
                        append: operation[2][1]
                    }
                    await this._emplace(page)
                    await this._emplace(right)
                }
                break
            case 'splice': {
                    const page = await this._journalist.read(operation.id)
                    page.items.splice.apply(page.items, operation.splice)
                    await this._emplace(page)
                }
                break
            case 'drain': {
                    // Ugh. Why is `operation` an array? Why can't it be an
                    // object so that the properties serve as a reminder?
                    //
                    // Interesting. I'm straight up reading and writing the
                    // files, which makes sense I suppose. Is this not a problem
                    // for the leaves, though?
                    const root = this._journalist.read('0.0')
                    const right = {
                        id: operation[2],
                        items: root.splice(operation[1])
                    }
                    const left = {
                        id: operation[3],
                        items: root.splice(0)
                    }
                    root.items = [{
                        id: left.id,
                        key: null
                    }, {
                        id: right.id,
                        key: right.items[0].key
                    }]
                    right.items[0].key = null
                    await this._emplace(root)
                    await this._emplace(left)
                    await this._emplace(right)
                }
                break
            case 'fill': {
                    const root = await this._journalist.read('0.0')
                    const page = await this._journalist.read(items[0].id)
                    root.items = page.items
                    await this._emplace(root)
                    await this._prepare([ 'unlink', path.join('pages', page.id) ])
                }
                break
            case 'unlink': {
                    await this._prepare(operation)
                }
                break
            }
        }
        await this._prepare([ 'end' ])
        return true
    }


    // Appears that prepared files are always going to be a decimal integer
    // followed by a hexidecimal integer. Files for emplacement appear to have a
    // hyphen in them.
    //
    async commit () {
        const dir = await this._readdir()
        const steps = dir.filter(file => {
            return /^\d+\.[0-9a-f]+$/.test(file)
        }).map(file => {
            const split = file.split('.')
            return { index: +split[0], file: file, hash: split[1] }
        }).sort((left, right) => left.index - right.index)
        for (const step of steps) {
            const operation = (await this._load(step.file)).shift()
            switch (operation.shift()) {
            case 'begin':
                const commit = dir.filter(function (file) {
                    return /^commit\./.test(file)
                }).shift()
                await fs.unlink(this._path(commit))
                break
            case 'rename':
                const from = path.join(this._journalist.directory, operation.shift())
                const to = path.join(this._journalist.directory, operation.shift())
                await fs.mkdir(path.dirname(to), { recursive: true })
                await fs.rename(from, to)
                const buffer = await fs.readFile(to)
                const hash = fnv(buffer)
                Strata.Error.assert(hash == operation.shift(), 'rename failed')
                break
            case 'unlink':
                await this._unlink(path.join(this._journalist.directory, operation.shift()))
                break
            case 'end':
                break
            }
            await fs.unlink(this._path(step.file))
        }
    }

    async dispose () {
        await this._unlink(this._commit)
    }
}

module.exports = Commit
