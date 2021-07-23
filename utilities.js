const path = require('path')
const fileSystem = require('fs')
const fs = require('fs').promises
const recorder = require('transcript/recorder').create(() => '0')

const { coalesce } = require('extant')

const FileSystem = require('./filesystem')

function recordify (header, parts) {
    return recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
}

const appendable = require('./appendable')

exports.directory = path.resolve(__dirname, './test/tmp')

exports.reset = async function (directory) {
    await coalesce(fs.rm, fs.rmdir).call(fs, directory, { recursive: true })
    await fs.mkdir(directory, { recursive: true })
}

exports.vivify = async function (directory) {
    const reader = new FileSystem.Reader(directory)
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

exports.serialize = async function (directory, files) {
    let instance = 0
    for (const id in files) {
        instance = Math.max(+id.split('.')[0], instance)
        await fs.mkdir(path.resolve(directory, 'pages', id), { recursive: true })
        if (+id.split('.')[1] % 2 == 0) {
            const buffers = files[id].map((record, index) => {
                return recordify({
                    method: 'insert',
                    index: index,
                    id: record[0]
                }, record[1] != null ? [ Buffer.from(JSON.stringify(record[1])) ] : [])
            })
            buffers.push(recordify({ method: 'length', length: files[id].length }, []))
            const buffer = Buffer.concat(buffers)
            const file = path.resolve(directory, 'pages', id, 'page')
            await fs.writeFile(file, buffer)
        } else {
            let key = null
            const writes = files[id].map((record, index) => {
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
                    console.log('?', record)
                    break
                }
            }).map(entry => recordify(entry.header, entry.parts))
            if (key != null) {
                writes.push(recordify({ method: 'key' }, key))
            }
            const file = path.resolve(directory, 'pages', id, '0.0')
            await fs.writeFile(file, Buffer.concat(writes))
        }
    }
    await fs.mkdir(path.resolve(directory, 'instances', String(instance)), { recursive: true })
    await fs.mkdir(path.resolve(directory, 'balance'))
}

exports.alphabet = function (length, letters = 26) {
    const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('').slice(0, letters)
    function append (length) {
        if (length == 1) {
            return alphabet.map(letter => [ letter ])
        }
        const entries = []
        for (const letter of alphabet) {
            for (const appendage of append(length - 1)) {
                entries.push(([ letter ]).concat(appendage))
            }
        }
        return entries
    }
    return append(length).map(word => word.join(''))
}
