// Node.js API.
const assert = require('assert')
const fs = require('fs').promises
const path = require('path')
const os = require('os')

// A non-cryptographic hash to assert the validity of the contents of a file.
const fnv = require('./fnv')

// Catch exceptions by type, message, property.
const rescue = require('rescue')

// An error to raise if journaling fails.
const Strata = { Error: require('./error') }

// Commit is a utility for atomic file operations leveraging the atomicity of
// `unlink` and `rename` on UNIX filesystems. With it you can perform a set of
// file operations that must occur together or not at all as part of a
// transaction. You create a script of file operations and once you commit the
// script you can be certain that the operations will be performed even if your
// program crash restarts, even if the crash restart is due to a full disk.
//
// Once you successfully commit, if the commit fails due to a full disk it can
// be resumed once you've made space on the disk and restarted your program.
//
// Commit has resonable limitations because it is primarily an atomicity utility
// and not a filesystem utility.
//
// Commit operates on a specific directory and will not operate outside the
// directory. It will not work across UNIX filesystems, so the directory should
// not include mounted filesystems or if it does, do not use Commit to write to
// those mounted filesystems.
//
// Commit file path are arguments must be relative paths. Commit does not
// perform extensive checking of those paths, it assumes that you've performed
// path sanitation and are not using path names entered from a user interface. A
// relative path that resolves to files outside of the directory is certain to
// cause problems. Also, I've only ever use old school UNIX filenames in the
// ASCII so I'm unfamiliar with the pitfalls of internationalization, emojiis
// and the like.

//
class Commit {
    constructor (directory, { tmp = 'commit', prepare = [] } = {}) {
        assert(typeof directory == 'string')
        this._index = 0
        this.directory = directory
        this._staged = {}
        this._commit = path.join(directory, tmp)
        this._tmp = {
            directory: tmp,
            path: path.join(directory, tmp)
        }
        this.__prepare = prepare
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
        this.__prepare.push({ method: 'unlink', path: filename })
    }

    _unlink (file) {
        return fs.rmdir(file, { recursive: true })
    }

    async _filename (filename) {
        const relative = path.normalize(filename)
        if (this._staged[relative]) {
            return this._staged[filename]
        }
        try {
            await fs.stat(path.join(this.directory, relative))
            return { relative, staged: false, operation: null }
        } catch (error) {
            rescue(error, [{ code: 'ENOENT' }])
            return null
        }
    }

    async filename (filename) {
        return (await this._filename(filename)).relative
    }

    async writeFile (formatter, buffer, { overwrite = false } = {}) {
        const hash = fnv(buffer)
        const abnormal = typeof formatter == 'function' ? formatter({ hash, buffer }) : formatter
        const filename = path.normalize(abnormal)
        if (this._staged[filename]) {
            if (!overwrite) {
                const error = new Error
                error.code = 'EEXISTS'
                error.errno = -os.constants.errno.EEXISTS
                error.path = filename
                throw error
            }
            const stat = fs.stat(this._staged[filename])
            if (stat.isDirectory()) {
                const error = new Error
                error.code = 'EISDIR'
                error.errno = -os.constants.errno.EISDIR
                error.path = filename
                throw error
            }
            await fs.unlink(this._stages[filename])
        }
        const temporary = path.join(this._commit, filename)
        await fs.mkdir(path.dirname(temporary), { recursive: true })
        await fs.writeFile(temporary, buffer)
        const operation = { method: 'emplace', filename, overwrite, hash }
        this._staged[filename] = {
            staged: true,
            relative: path.join(this._tmp.directory, filename),
            operation: operation
        }
        this.__prepare.push(operation)
        return operation
    }

    async mkdir (dirname, { overwrite = false }) {
        const temporary = path.join(this._commit, formatted)
        await fs.mkdir(temporary, { recursive: true })
        return { method: 'emplace', filename, overwrite, hash: null }
    }

    _error (error, code, path) {
        error.code = 'EISDIR'
        error.errno = -os.constants.errno.EISDIR
        error.path = filename
        Error.captureStackTrace(error, Commit.prototype._error)
        throw error
    }

    // This file operation will create any directory specified in the
    // destination path.

    //
    async rename (from, to, { overwrite = false } = {}) {
        const resolved = {
            from: await this._filename(from),
            to: await this._filename(to)
        }
        if (resolved.to && resolved.to.staged) {
            if (!overwrite) {
                this._error('EEXISTS', to)
            }
            fs.unlink(resolved.to.filename, { recursive: true })
        }
        if (resolved.from.staged) {
            await fs.mkdir(path.dirname(from), { recursive: true })
            // TODO How do I update the 'emplace' or rename?
            const temporary = {
                from: path.join(this.directory, resolved.from.relative),
                to: path.join(this._tmp.path, to)
            }
            await fs.rename(temporary.from, temporary.to)
            resolved.from.operation.filename = path.join(this._tmp.directory, to)
        } else {
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
                    const filename = path.join(path.basename(this._commit), `commit.${hash}`)
                    await fs.mkdir(path.dirname(path.join(this._commit, filename)), { recursive: true })
                    await fs.writeFile(path.join(this._commit, filename), buffer)
                    await this._prepare([ 'rename', filename, hash ])
                }
                break
            case 'emplace': {
                    const { page, hash, filename, overwrite } = operation
                    if (overwrite) {
                        await this._prepare([ 'unlink', filename ])
                    }
                    await this._prepare([ 'rename', filename, hash ])
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
            case 'rename': {
                    const filename = operation.shift()
                    const from = path.join(this._commit, filename)
                    const to = path.join(this.directory, filename)
                    await fs.mkdir(path.dirname(to), { recursive: true })
                    // When replayed from failure we'll get `ENOENT`.
                    await fs.rename(from, to)
                    const buffer = await fs.readFile(to)
                    const hash = fnv(buffer)
                    Strata.Error.assert(hash == operation.shift(), 'rename failed')
                }
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
