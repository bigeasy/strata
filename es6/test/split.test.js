describe('strata split', () => {
    const assert = require('assert')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    const fs = require('fs').promises
    before(() => utilities.reset(directory))
    it('can split a leaf page', async () => {
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
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const cursor = (await strata.search('f')).get()
        cursor.insert('f', 'f', cursor.index)
        cursor.release()
        await cursor.flush()
        return
        cursor.remove(cursor.index)
        cursor.release()
        await cursor.flush()
        await strata.close()
        const vivified = await utilities.vivify(directory)
        assert.deepStrictEqual(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': [
                [ 'insert', 0, 'a' ],
                [ 'insert', 1, 'b' ],
                [ 'insert', 2, 'c' ],
                [ 'delete', 0 ]
            ]
        }, 'inserted')
        cache.purge(0)
        assert(cache.heft, 0, 'cache purged')
    })
})
