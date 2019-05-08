const path = require('path')
const callback = require('../callback')
const rimraf = require('rimraf')
const ascension = require('ascension')
const fileSystem = require('fs')
const fs = require('fs').promises
const shifter = require('./shifter')(() => '0')
const recorder = require('../recorder')(() => '0')

const appendable = ascension([ Number, Number ], function (file) {
    return file.split('.')
})

exports.directory = path.resolve(__dirname, './tmp')

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
        const append = dir.filter(function (file) {
            return /^\d+\.\d+$/.test(file)
        }).sort(appendable).pop()
        const lines = (await fs.readFile(path.resolve(pages, file, append), 'utf8')).split(/\n/)
        lines.pop()
        const entries = lines.map(line => JSON.parse(line))
        if (+file.split('.')[1] % 2 == 1) {
            var records = []
            while (entries.length != 0) {
                var record = shifter(entries), header = record[0]
                switch (header.method) {
                case 'insert':
                    records.push({ method: header.method, index: header.index, body: record[1] })
                    break
                case 'remove':
                    records.push({ method: header.method, index: header.index })
                    break
                }
            }
            vivified[file] = records
        } else {
            var records = []
            while (entries.length != 0) {
                var record = shifter(entries), header = record[0]
                switch (header.method) {
                case 'insert':
                    records.splice(header.index, 0, [ header.value.id, header.value.key ])
                    break
                }
            }
            vivified[file] = records
        }
    }
    return vivified
}

exports.serialize = async function (directory, files) {
    let instance = 0
    for (let id in files) {
        instance = Math.max(+id.split('.')[0], instance)
        await fs.mkdir(path.resolve(directory, 'pages', id), { recursive: true })
        const writes = (
            +id % 2 == 0 ? (
                files[id].map((record, index) => {
                    return {
                        header: {
                            method: 'insert',
                            index: index,
                            value: { id: record[0], key: record[1] }
                        },
                        body: null
                    }
                })
            ) : (
                files[id].map((record, index) => {
                    return {
                        header: {
                            method: record.method,
                            index: record.index
                        },
                        body: {
                            key: record.body,
                            value: record.body
                        }
                    }
                })
            )
        ).map(entry => recorder(entry.header, entry.body))
        const file = path.resolve(directory, 'pages', id, '0.0')
        await fs.appendFile(file, Buffer.concat(writes))
    }
    await fs.mkdir(path.resolve(directory, 'instances', String(instance)), { recursive: true })
}
