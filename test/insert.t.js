require('proof')(2, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('insert.t')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'insert')
    const fs = require('fs').promises
    await utilities.reset(directory)
    const cache = new Cache
    const strata = new Strata(destructible.durable('strata'), { directory, cache })
    await strata.create()
    const cursor = (await strata.search('a')).get()
    cursor.insert('a', 'a', cursor.index)
    cursor.insert('b', 'b', ~cursor.indexOf('b', cursor.index))
    cursor.release()
    await cursor.flush()
    await strata.close()
    cache.purge(0)
    okay(cache.heft, 0, 'cache empty')
    const vivified = await utilities.vivify(directory)
    okay(vivified, {
        '0.0': [ [ '0.1', null ] ],
        '0.1': [
            [ 'insert', 0, 'a' ],
            [ 'insert', 1, 'b' ]
        ]
    }, 'inserted')
    await destructible.destructed
})
