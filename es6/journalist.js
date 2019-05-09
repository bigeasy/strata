const ascension = require('ascension')
const fileSystem = require('fs')
const fs = require('fs').promises
const path = require('path')
const recorder = require('./recorder')
const Interrupt = require('interrupt').createInterrupter('strata')

const appendable = require('./appendable')

class Journalist {
    constructor (options) {
        this.cache = options.cache
        this.instance = 0
        this.options = options
        this._recorder = recorder(() => '0')
        this._root = null
    }

    async create () {
        const directory = this.options.directory
        this._root = this.cache.hold([ directory, -1 ], { items: [{ id: '0.0' }] })
        const stat = await fs.stat(directory)
        Interrupt.assert(stat.isDirectory(), 'create.not.directory', { directory: directory })
        const dir = await fs.readdir(this.options.directory)
        Interrupt.assert((await fs.readdir(this.options.directory)).filter(file => {
            return ! /^\./.test(file)
        }).length == 0, 'create.directory.not.empty', { directory: directory })
        await fs.mkdir(path.resolve(directory, 'instance', '0'), { recursive: true })
        const pages = path.resolve(this.options.directory, 'pages')
        await fs.mkdir(path.resolve(pages, '0.0'), { recursive: true })
        await fs.writeFile(path.resolve(pages, '0.0', '0.0'), this._recorder.call(null, {
            method: 'insert',
            index: 0,
            value: { id: '0.1', key: null }
        }))
        await fs.mkdir(path.resolve(pages, '0.1'), { recursive: true })
        await fs.writeFile(path.resolve(pages, '0.1', '0.0'), Buffer.alloc(0))
    }

    async open () {
        const directory = this.options.directory
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

    async _appendable (id) {
        const dir = await fs.readdir(path.join(this.options.directory, 'pages', id))
        return dir.filter(function (file) {
            return /^\d+\.\d+$/.test(file)
        }).sort(appendable).pop()
    }

    async read (id) {
        const directory = path.resolve(this.directory, 'pages', String(id))
        const items = [], leaf = +id.split('.')[1] % 2 == 1
        let heft = 0
        const splitter = new Splitter(function () { return '0' })
        const append = this._appendable(id)
        const filename = path.join(directory, append)
        const readable = fileSystem.createReadStream(filename)
        for await (let chunk of readable) {
            splitter.split(chunk).forEach(function (entry) {
                switch (entry.header.method) {
                case 'insert':
                    if (leaf) {
                        items.splice(entry.header.index, 0, {
                            key: entry.body.key,
                            value: entry.body.value,
                            heft: entry.sizes[1]
                        })
                        heft += entry.sizes[1]
                    } else {
                        items.splice(entry.header.index, 0, {
                            id: entry.header.value.id,
                            key: entry.header.value.key,
                            heft: entry.sizes[0]
                        })
                        heft += entry.sizes[0]
                    }
                }
            })
        }
        // TODO Did we ghost? Check when we implement remove.
        return { id, leaf, items, ghosts: 0, heft, append }
    }

    async load (id) {
        const entry = this._cache.hold(id, null)
        try {
            if (entry.value == null) {
                entry.value = await this.read(id)
                entry.heft = entry.value.heft
            }
        } finally {
            entry.release()
        }
    }

    _descend (key, level, fork) {
        const descent = { miss: null, entries: [], index: 0, level: 0, keyed: null }
        let entry = null
        descent.entries.push(entry = this._cache.hold(-1, null))
        for (;;) {
            if (descent.index != 0) {
                descent.keyed = {
                    key: page.items[descent.index].key,
                    level: descent.level
                }
            }
            var id = entry.value.items[descent.index].id
            descent.entries.push(entry = this._cache.hold(id, null))
            if (entry.value == null) {
                descent.entries.pop().remove()
                descent.miss = id
                return descent
            }
            var page = entry.value
            // TODO Maybe page offset instead of ghosts, nah leave it so you remember it.
            descent.index = find(this.options.comparator, page, key, page.leaf ? page.ghosts : 1)
            if (page.leaf) {
                assert.equal(level, -1, 'could not find branch')
                break
            } else if (level == descent.level) {
                break
            } else if (descent.index < 0) {
                // On a branch, unless we hit the key exactly, we're
                // pointing at the insertion point which is right after the
                // branching we're supposed to decend, so back it up one
                // unless it's a bullseye.
                descent.index = ~descent.index - 1
            } else if (fork != 0) {
                if (fork < 0) {
                    if (descent.index-- == 0) {
                        return null
                    }
                } else {
                    if (++descent.index == page.items.length) {
                        return null
                    }
                }
            }
            descent.level++
        }
        return descent
    }

    async descend (key, level, fork) {
        let entries = []
        for (;;) {
            var descent = this._descend(key, level, fork)
            entries.forEach((entry) => entry.release())
            if (descent.miss == null) {
                return descent
            }
            entries = descent.entries
            await this.load(descent.miss)
        }
    }

    async close () {
        if (this._root != null) {
            this._root.remove()
            this._root = null
        }
    }
}

module.exports = Journalist