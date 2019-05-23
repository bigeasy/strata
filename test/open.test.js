describe('strata open', () => {
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
    it('can create a new database', async () => {
        await utilities.serialize(directory, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': []
        })
        await fs.mkdir(path.join(directory, 'instances', '1'))
        await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const instances = await fs.readdir(path.join(directory, 'instances'))
        assert.deepStrictEqual(instances, [ '2' ], 'instance')
        assert.equal(cache.entries, 1, 'cache empty')
        await strata.close()
        await strata.close()
        cache.purge(0)
        assert.equal(cache.entries, 0, 'cache empty')
    })
})
