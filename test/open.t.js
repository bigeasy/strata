require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('open.t')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'open')
    const fs = require('fs').promises
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [ [ '0.1', null ] ],
        '0.1': []
    })
    await fs.mkdir(path.join(directory, 'instances', '1'))
    await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))
    const cache = new Cache
    const strata = new Strata(destructible.durable('strata'), { directory, cache })
    await strata.open()
    const instances = await fs.readdir(path.join(directory, 'instances'))
    okay(instances, [ '2' ], 'instance')
    okay(cache.entries, 1, 'cache empty')
    await strata.close()
    await strata.close()
    cache.purge(0)
    okay(cache.entries, 0, 'cache empty')
    await destructible.rejected
})
