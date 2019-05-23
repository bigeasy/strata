describe('strata create', () => {
    const assert = require('assert')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'create')
    const fs = require('fs').promises
    before(async () => {
        await utilities.reset(directory)
    })
    it('can create a new database', async () => {
        await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.create()
        await strata.close()
        await strata.close()
        const vivified = await utilities.vivify(directory)
        assert.deepStrictEqual(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': []
        }, 'created')
        cache.purge(0)
        assert.equal(cache.entries, 0, 'cache empty')
    })
})
