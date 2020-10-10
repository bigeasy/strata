// Node.js API.
const assert = require('assert')
const fs = require('fs').promises
const path = require('path')

// A non-cryptographic hash to assert the validity of the contents of a file.
const fnv = require('./fnv')

// An error to raise if journaling fails.
const Strata = { Error: require('./error') }

class Commit {
    constructor (directory, { tmp = 'commit' } = {}) {
        assert(typeof directory == 'string')
        this._index = 0
        this.directory = directory
        this._commit = path.join(directory, tmp)
    }

    // TODO Should be a hash of specific files to filter, not a regex.
    async write (prepare) {
        const dir = await this._readdir()
        const unemplaced = dir.filter(file => ! /\d+\.\d+-\d+\.\d+\.[0-9a-f]/)
        assert.deepStrictEqual(unemplaced, [], 'commit directory not empty')
        await this._write('commit', prepare)
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

    unlink (filename) {
        return { method: 'unlink', path: filename }
    }

    _unlink (file) {
        return fs.rmdir(file, { recursive: true })
    }

    async writeFile (formatter, buffer, { overwrite = false } = {}) {
        const hash = fnv(buffer)
        const filename = typeof formatter == 'function' ? formatter({ hash, buffer }) : formatter
        const temporary = path.join(this._commit, filename)
        await fs.mkdir(path.dirname(temporary), { recursive: true })
        await fs.writeFile(temporary, buffer)
        return { method: 'emplace2', filename, overwrite, hash }
    }

    async mkdir (dirname, { overwrite = false }) {
        const temporary = path.join(this._commit, formatted)
        await fs.mkdir(temporary, { recursive: true })
        return { method: 'emplace2', filename, overwrite, hash: null }
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
            case 'rename': {
                    const { from, to, hash } = operation
                    await this._prepare([ 'rename', from, to, hash ])
                }
                break
            case 'emplace2': {
                    const { page, hash, filename, overwrite } = operation
                    if (overwrite) {
                        await this._prepare([ 'unlink2', filename ])
                    }
                    await this._prepare([ 'rename2', filename, hash ])
                }
                break
            case 'unlink': {
                    await this._prepare([ 'unlink', operation.path ])
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
            case 'rename2': {
                    const filename = operation.shift()
                    const from = path.join(this._commit, filename)
                    const to = path.join(this.directory, filename)
                    await fs.mkdir(path.dirname(to), { recursive: true })
                    await fs.rename(from, to)
                    const buffer = await fs.readFile(to)
                    const hash = fnv(buffer)
                    Strata.Error.assert(hash == operation.shift(), 'rename failed')
                }
                break
            case 'rename':
                const from = path.join(this.directory, operation.shift())
                const to = path.join(this.directory, operation.shift())
                await fs.mkdir(path.dirname(to), { recursive: true })
                // When replayed from failure we'll get `ENOENT`.
                await fs.rename(from, to)
                const buffer = await fs.readFile(to)
                const hash = fnv(buffer)
                Strata.Error.assert(hash == operation.shift(), 'rename failed')
                break
            case 'unlink2':
                await this._unlink(path.join(this.directory, operation.shift()))
                break
            case 'unlink':
                await this._unlink(path.join(this.directory, operation.shift()))
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
