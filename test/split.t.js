require('proof')(1, async (okay) => {
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    const fs = require('fs').promises
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ], [
            'insert', 3, 'd'
        ], [
            'insert', 4, 'e'
        ]]
    })
    async function split () {
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const cursor = (await strata.search('f')).get()
        cursor.insert('f', 'f', cursor.index)
        cursor.release()
        await cursor.flush()
        await new Promise(resolve => setTimeout(resolve, 50))
        await strata.close()
        cache.purge(0)
        // **TODO** broken test...
        // okay(cache.heft, 0, 'cache purged')
    }
    await split()
    async function reopen () {
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const cursor = (await strata.search('f')).get()
        okay(cursor.items[cursor.index].value, 'f', 'found')
        cursor.release()
        await strata.close()
    }
    await reopen()
})
