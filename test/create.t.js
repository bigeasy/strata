require('proof')(2, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('create.t')

    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'create')
    const fs = require('fs').promises

    await utilities.reset(directory)

    await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))
    const cache = new Cache
    const strata = new Strata(destructible.durable('strata'), { directory, cache })
    await strata.create()
    await strata.close()
    await strata.close()
    const vivified = await utilities.vivify(directory)
    okay(vivified, {
        '0.0': [ [ '0.1', null ] ],
        '0.1': []
    }, 'created')
    cache.purge(0)
    okay(cache.entries, 0, 'cache empty')
    await destructible.destructed
})
