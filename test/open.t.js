require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const Strata = require('../strata')
    const Turnstile = require('turnstile')
    const Magazine = require('magazine')
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
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, turnstile  })
            const instances = await fs.readdir(path.join(directory, 'instances'))
            okay(instances, [ '2' ], 'instance')
            okay(pages.size, 1, 'cache not empty')
            destructible.destroy()
        })
        await destructible.promise
        pages.purge(0)
        okay(pages.size, 0, 'cache empty')
    }
})
