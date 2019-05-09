describe('strata get', () => {
    const assert = require('assert')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'open')
    const fs = require('fs').promises
    before(async () => {
        await utilities.reset(directory)
    })
    it('can get a cursor', async () => {
        await utilities.serialize(directory, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': [{ "method": "insert", "index": 0, "body": "a" }]
        })
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const search = await strata.search('a')
        const cursor = search.get()
        assert(search.get() === cursor, 'get again')
        assert.deepStrictEqual(cursor.items[cursor.index], {
            key: 'a', value: 'a', heft: 23
        }, 'got')
        cursor.release()
        await strata.close()
        cache.purge(0)
        assert.equal(cache.heft, 0, 'cache purged')
    })
})
