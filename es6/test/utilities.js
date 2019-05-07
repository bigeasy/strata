const path = require('path')
const callback = require('../callback')
const rimraf = require('rimraf')

exports.directory = path.resolve(__dirname, './tmp')

exports.reset = async function (directory) {
    await callback(callback => rimraf(directory, callback))
    await fs.mkdir(directory, { recursive: true })
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
                            value: { id: record.id, key: record.key }
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
        ).map((entry) => recorder(entry.header, entry.body))
        const file = path.resolve(directory, 'pages', id, '0.0')
        const stream = fs.createFileStream(file, { flags: 'a' })
        const appender = new Appender(stream)
        await appender.append(writes)
        await appender.end()
    }
    await fs.mkdir(path.resolve(directory, 'instance', String(instance)), { recursive: true })
}
