require('proof')(3, async (okay) => {
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'get')
    const fs = require('fs').promises
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[ 'insert', 0, 'a' ]]
    })
    const cache = new Cache
    const strata = new Strata({ directory, cache })
    await strata.open()
    const search = await strata.search('a')
    const cursor = search.get()
    okay(search.get() === cursor, 'get again')
    okay(cursor.items[cursor.index], {
        key: 'a', value: 'a', heft: 76
    }, 'got')
    cursor.release()
    await strata.close()
    cache.purge(0)
    okay(cache.heft, 0, 'cache purged')
})
