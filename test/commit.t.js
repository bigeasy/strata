require('proof')(2, async okay => {
    const fs = require('fs').promises
    const path = require('path')

    const Commit = require('../commit')

    const directory = path.join(__dirname, 'tmp', 'commit')
    await fs.rmdir(directory, { recursive: true })
    const commit = new Commit(directory)

    const entry = await commit.writeFile('hello/world.txt', Buffer.from('hello, world'))
    okay(entry, {
        method: 'emplace',
        filename: 'hello/world.txt',
        overwrite: false,
        hash: '4d0ea41d'
    }, 'write file')
    okay(await commit.filename('hello/world.txt'), 'commit/hello/world.txt', 'aliased')
})
