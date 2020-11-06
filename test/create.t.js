require('proof')(4, async (okay) => {
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'create')
    const fs = require('fs').promises

    await utilities.reset(directory)
    await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))

    const cache = new Cache

    {
        const strata = await Strata.open(new Destructible('strata'), { directory, cache, create: true  })
        await strata.destructible.destroy().rejected
        const vivified = await utilities.vivify(directory)
        okay(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': []
        }, 'created')
        cache.purge(0)
        okay(cache.entries, 0, 'cache empty')

        okay(strata.compare('a', 'a'), 0, 'compare')
        okay(strata.extract([ 'a' ]), 'a', 'extract')
    }

    {
        const strata = await Strata.open(new Destructible('strata'), { directory, cache })
        await strata.destructible.destroy().rejected
    }
})
