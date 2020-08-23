const path = require('path')
const callback = require('prospective/callback')
const rimraf = require('rimraf')
const ascension = require('ascension')
const fileSystem = require('fs')
const fs = require('fs').promises
const shifter = require('./shifter')(() => '0')
const recorder = require('./recorder')(() => '0')
const fnv = require('./fnv')

const appendable = ascension([ Number, Number ], function (file) {
    return file.split('.')
})

exports.directory = path.resolve(__dirname, './test/tmp')

exports.reset = async function (directory) {
    await callback(callback => rimraf(directory, callback))
    await fs.mkdir(directory, { recursive: true })
}

exports.vivify = async function (directory) {
    const vivified = {}
    const pages = path.join(directory, 'pages')
    for (let file of await fs.readdir(pages)) {
        if (!/^\d+.\d+$/.test(file)) {
            continue
        }
        const dir = await fs.readdir(path.resolve(directory, 'pages', file))
        if (+file.split('.')[1] % 2 == 1) {
            const append = dir.filter(function (file) {
                return /^\d+\.\d+(?:\.[0-9a-f]+)?$/.test(file)
            }).sort(appendable).pop()
            const lines = (await fs.readFile(path.resolve(pages, file, append), 'utf8')).split(/\n/)
            lines.pop()
            const entries = lines.map(line => JSON.parse(line))
            const records = []
            while (entries.length != 0) {
                const record = shifter(entries), header = record[0].header
                switch (header.method) {
                case 'right':
                    records.push([ header.method, header.right ])
                    break
                case 'insert':
                    records.push([ header.method, header.index, record[1] ])
                    break
                case 'delete':
                    records.push([ header.method, header.index ])
                    break
                case 'load': {
                        const { id, append } = header
                        const load = (await fs.readFile(path.resolve(pages, id, append), 'utf8')).split(/\n/)
                        load.pop()
                        entries.unshift.apply(entries, load.map(line => JSON.parse(line)))
                    }
                    break
                }
            }
            vivified[file] = records
        } else {
            const hash = dir.filter(file => /^[0-9a-f]+$/.test(file)).pop()
            const lines = (await fs.readFile(path.resolve(pages, file, hash), 'utf8')).split(/\n/)
            lines.pop()
            const entries = lines.map(line => JSON.parse(line))
            const items = []
            while (entries.length) {
                const record = shifter(entries)
                items.push([ record[0].header.id, record.length == 2 ? record[1] : null ])
            }
            vivified[file] = items
        }
    }
    return vivified
}

exports.serialize = async function (directory, files) {
    let instance = 0
    for (const id in files) {
        instance = Math.max(+id.split('.')[0], instance)
        await fs.mkdir(path.resolve(directory, 'pages', id), { recursive: true })
        if (+id.split('.')[1] % 2 == 0) {
            const buffers = files[id].map(record => {
                return recorder({
                    id: record[0]
                }, record[1] != null ? [ Buffer.from(JSON.stringify(record[1])) ] : [])
            })
            const buffer = Buffer.concat(buffers)
            const hash = fnv(buffer)
            const file = path.resolve(directory, 'pages', id, hash)
            await fs.writeFile(file, buffer)
        } else {
            const writes = files[id].map((record, index) => {
                switch (record[0]) {
                case 'right':
                    return {
                        header: { method: 'right' },
                        parts: [ Buffer.from(JSON.stringify(record[1])) ]
                    }
                case 'insert':
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
            }).map(entry => recorder(entry.header, entry.parts))
            const file = path.resolve(directory, 'pages', id, '0.0')
            await fs.writeFile(file, Buffer.concat(writes))
        }
    }
    await fs.mkdir(path.resolve(directory, 'instances', String(instance)), { recursive: true })
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
