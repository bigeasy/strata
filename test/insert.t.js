require('proof')(2, async (okay) => {
    const path = require('path')

    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')

    const directory = path.join(utilities.directory, 'insert')
    await utilities.reset(directory)

    const cache = new Cache
    const strata = new Strata(new Destructible('strata'), { directory, cache })

    await strata.create()

    const writes = {}

    const cursor = await strata.search('a')
    const { index } = cursor.indexOf('a')
    cursor.insert(index, 'a', [ 'a' ], writes)
    cursor.insert(cursor.indexOf('b', index).index, 'B', [ 'b' ], writes)
    cursor.release()

    Strata.flush(writes)
    await strata.destructible.destroy().rejected

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
})
