require('proof')(2, async (okay) => {
    const path = require('path')

    const Trampoline = require('reciprocate')
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

    const trampoline = new Trampoline
    strata.search(trampoline, 'a', cursor => {
        cursor.insert(cursor.index, 'a', [ 'a' ], writes)
        cursor.insert(cursor.indexOf('b', cursor.index).index, 'B', [ 'b' ], writes)
    })
    while (trampoline.seek()) {
        await trampoline.shift()
    }

    await Strata.flush(writes)
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
