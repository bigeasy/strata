'use strict'

const Destructible = require('destructible')
const Turnstile = require('turnstile')

const Fracture = require('fracture')
const Strata = require('../strata')
const Magazine = require('magazine')
const Player = require('transcript/player')
const Operation = require('operation')
const WriteAhead = require('writeahead')

const utilities = require('../utilities')
const path = require('path')
const fs = require('fs').promises
const recorder = require('transcript/recorder').create(() => '0')

function recordify (header, parts) {
    return recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
}

async function waserialize (writeahead, files) {
    let instance = 0
    const writes = [{
        header: {
            method: 'apply',
            key: 'instance'
        },
        parts: []
    }, {
        header: {
            method: 'instance',
            instance: 0
        },
        parts: []
    }].map(entry => recordify(entry.header, entry.parts))
    await writeahead.write(Fracture.stack(), [{ keys: [[ 0, 'instance' ]], buffer: Buffer.concat(writes) }])
    for (const id in files) {
        instance = Math.max(+id.split('.')[0], instance)
        if (+id.split('.')[1] % 2 == 1) {
            let key = null
            const writes = [{
                header: {
                    method: 'apply',
                    key: id
                },
                parts: []
            }].concat(files[id].map((record, index) => {
                switch (record[0]) {
                case 'right':
                    return {
                        header: { method: 'right' },
                        parts: [ Buffer.from(JSON.stringify(record[1])) ]
                    }
                case 'insert':
                    if (record[1] == 0 && id != '0.1') {
                        key = [ Buffer.from(JSON.stringify(record[2])) ]
                    }
                    return {
                        header: { method: 'insert', index: record[1] },
                        parts: [ Buffer.from(JSON.stringify(record[2])) ]
                    }
                case 'delete':
                    return {
                        header: { method: 'delete', index: record[1] },
                        parts: []
                    }
                default:
                    console.log(record)
                    break
                }
            })).map(entry => recordify(entry.header, entry.parts))
            if (key != null) {
                writes.push(recordify({ method: 'key' }, key))
            }
            await writeahead.write(Fracture.stack(), [{ keys: [[ 0, id ]], buffer: Buffer.concat(writes) }])
        } else {
            const writes = [{
                header: {
                    method: 'apply',
                    key: id
                },
                parts: []
            }].concat(files[id].map((record, index) => {
                const parts = index != 0
                    ? [ Buffer.from(JSON.stringify(record[1])) ]
                    : []
                return {
                    header: {
                        method: 'insert',
                        index: index,
                        id: record[0]
                    },
                    parts: parts
                }
            })).map(entry => recordify(entry.header, entry.parts))
            await writeahead.write(Fracture.stack(), [{ keys: [[ 0, id ]], buffer: Buffer.concat(writes) }])
        }
    }
}

const WriteAheadOnly = require('../writeahead')

async function walvivify (writeahead) {
    const reader = new WriteAheadOnly.Reader({ writeahead: writeahead, key: 0 })
    const vivified = {}
    async function vivify (id) {
        const { page } = await reader.page(id)
        if (page.leaf) {
            const items = vivified[id] = page.items.map((item, index) => [ 'insert', index, item.parts[0] ])
            if (page.right) {
                items.push([ 'right', page.right ])
            }
        } else {
            const items = vivified[id] = page.items.map(item => [ item.id, item.key == null ? null : item.key ])
            for (const item of items) {
                await vivify(item[0])
            }
        }
    }
    await vivify('0.0')
    return vivified
}

async function* test (suite, okay, only = [ 'fileSystem', 'writeahead' ]) {
    const directory = path.join(utilities.directory, suite)
    async function fileSystem ($trace, test, f, { create = false, serialize = null, vivify = null, comparator = null } = {}) {
        if (serialize != null) {
            await utilities.serialize(directory, serialize)
        }
        const FileSystem = require('../filesystem')
        const destructible = new Destructible({ $trace }, 'create.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), { isolated: true }, 'turnstile'))
        const pages = new Magazine
        const handles = new Operation.Cache(new Magazine)
        const storage = new FileSystem.Writer(destructible.durable($ => $(), 'filesystem'), await FileSystem.open({ directory, handles, create }))
        destructible.ephemeral($ => $(), `${suite}.${test}.fs`, async () => {
            const strata = new Strata(destructible.durable($ => $(), 'strata'), { pages, storage, turnstile, comparator  })
            await f({ strata, directory, prefix: [ suite, test, 'file system' ].join(' '), pages })
            destructible.destroy()
        })
        await destructible.promise
        pages.purge(0)
        await handles.shrink(0)
        okay(pages.size, 0, `${suite} ${test} file system pages empty`)
        okay(handles.magazine.size, 0, `${suite} ${test} file system handles empty`)
        if (vivify != null) {
            const vivified = await utilities.vivify(directory)
            okay(vivified, vivify, `${suite} ${test} file system vivify`)
        }
    }
    async function writeahead ($trace, test, f, { create = false, serialize = null, vivify = null, comparator = null } = {}) {
        const destructible = new Destructible({ $trace }, 'writeahead.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), { isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible, turnstile, await WriteAhead.open({ directory }))
        if (serialize != null) {
            await waserialize(writeahead, serialize)
        }
        writeahead.deferrable.increment()
        const pages = new Magazine
        const storage = new WriteAheadOnly.Writer(destructible.durable($ => $(), 'storage'), await WriteAheadOnly.open(Fracture.stack(), { writeahead, key: 0, create: create ? [ 'x', 'x' ] : null }))
        await destructible.ephemeral($ => $(), `${suite}.${test}.writeahead`, async () => {
            const strata = new Strata(destructible.durable($ => $(), 'strata'), { pages, storage, turnstile, comparator })
            await f({
                strata: strata,
                pages: pages,
                directory: directory,
                prefix: [ suite, test, 'writeahead only' ].join(' ')
            })
            await strata.drain()
            await writeahead.write(Fracture.stack(), [])
            writeahead.deferrable.decrement()
            destructible.destroy()
        })
        await destructible.promise
        pages.purge(0)
        okay(pages.size, 0, `${suite} ${test} writeahead only system pages empty`)
        if (vivify != null) {
            const vivified = await walvivify(writeahead)
            okay(vivified, vivify, `${suite} ${test} writeahead only vivify`)
        }
    }
    for (const harness of [ fileSystem, writeahead ]) {
        if (~only.indexOf(harness.name)) {
            await utilities.reset(directory)
            await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))
            yield harness
        }
    }
}

module.exports = test
