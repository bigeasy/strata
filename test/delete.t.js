require('proof')(2, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('delete.t')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'delete')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ]]
    })
    destructible.durable('test', async function () {
        const cache = new Cache
        const strata = new Strata(destructible.durable('strata'), { directory, cache })
        await strata.open()
        const cursor = (await strata.search('a')).get()
        cursor.remove(cursor.index)
        await cursor.flush()
        cursor.release()
        await strata.close()
        console.log(cache.entries)
        const vivified = await utilities.vivify(directory)
        okay(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': [
                [ 'insert', 0, 'a' ],
                [ 'insert', 1, 'b' ],
                [ 'insert', 2, 'c' ],
                [ 'delete', 0 ]
            ]
        }, 'inserted')
        cache.purge(0)
        // **TODO** Cache purge broken.
        okay(cache.heft, 0, 'cache purged')
    })
    await destructible.destructed
})
