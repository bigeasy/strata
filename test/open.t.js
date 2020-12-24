require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const Strata = require('../strata')
    const Turnstile = require('turnstile')
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

    {
        const destructible = new Destructible('load.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            const instances = await fs.readdir(path.join(directory, 'instances'))
            okay(instances, [ '2' ], 'instance')
            okay(cache.entries, 1, 'cache empty')
            destructible.destroy()
        })
        await destructible.rejected
        cache.purge(0)
        okay(cache.entries, 0, 'cache empty')
    }
})
