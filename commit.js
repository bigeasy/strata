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
        assert.deepStrictEqual(dir, [], 'commit directory not empty')
        await this._write('commit', commit)
    }

    async _prepare (operation) {
        await this._write(String(this._index++), [ operation ])
    }

    async _write (file, entries) {
        const directory = path.join(this._journalist.directory, 'commit')
        const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
        await fs.writeFile(path.join(directory, 'prepare'), buffer)
        const hash = fnv(buffer)
        const from = path.join(directory, 'prepare')
        const to = path.join(directory, `${file}.${hash}`)
        await fs.rename(from, to)
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

    async _emplace (page) {
        const unlink = path.join('pages', page.id, `${page.append}.${page.hash}`)
        const buffer = Buffer.from(JSON.stringify(page.items))
        const hash = fnv(buffer)
        const filename = `${page.append}.${hash}`
        await fs.writeFile(this._path(`${page.id}-${filename}`), buffer)
        const entry = this._journalist._hold(page.id)
        if (entry.value == null) {
            entry.remove()
        } else {
            entry.heft = buffer.length
            entry.append = page.append
            entry.release()
        }
        const from = path.join('commit', `${page.id}-${filename}`)
        const to = path.join('pages', page.id, filename)
        await this._prepare([ 'rename', from, to, hash ])
        await this._prepare([ 'unlink', unlink ])
    }

    async prepare (stop) {
        const dir = await this._readdir()
        const commit = dir.filter(file => /^commit\.[0-9a-f]+$/.test(file)).shift()
        if (commit == null) {
            return false
        }
        for (const file of dir.filter(file => file != commit)) {
            await this._unlink(this._path(file))
        }
        const operations = await this._load(commit)
        // Start by deleting the commit script, once this runs we have to move
        // forward through the entire commit.
        await this._prepare([ 'begin' ])
        while (operations.length != 0) {
            const operation = operations.shift()
            switch (operation[0]) {
            // This is the next commit in a series of commits, we write out the
            // remaining operations into a new commit.
            case 'commit': {
                    const entries = operations.splice(0)
                    const buffer = Buffer.from(entries.map(JSON.stringify).join('\n') + '\n')
                    const hash = fnv(buffer)
                    const filename = this._path('_commit')
                    await fs.writeFile(this._path('_commit'), buffer)
                    const from = path.join('commit', '_commit')
                    const to = path.join('commit', `commit.${hash}`)
                    await this._prepare([ 'rename', from, to, hash ])
                }
                break
            // Write out a stub leaf page that splits, merges or vacuums (simply
            // loads) a previous page.
            case 'stub': {
                    const recorder = this._journalist._recorder
                    const page = { id: operation[1], append: operation[2] }
                    const buffer = recorder(operation[3])
                    const hash = fnv(buffer)
                    const filename = `${page.id}-${page.append}`
                    const from = path.join('commit', filename)
                    const to = path.join('pages', page.id, page.append)
                    await fs.writeFile(this._path(filename), buffer)
                    await this._prepare([ 'rename', from, to, hash ])
                }
                break
            case 'split': {
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
                    const page = await this._journalist.read(operation[1])
                    page.items.splice.apply(page.items, operation[2])
                    await this._emplace(page)
                }
                break
            case 'drain': {
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
}

module.exports = Commit
