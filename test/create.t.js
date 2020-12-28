require('proof')(4, async (okay) => {
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Magazine = require('magazine')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'create')
    const fs = require('fs').promises

    await utilities.reset(directory)
    await fs.writeFile(path.join(directory, '.ignore'), Buffer.alloc(0))


    {
        const destructible = new Destructible('create.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile, create: true  })
            okay(strata.compare('a', 'a'), 0, 'compare')
            okay(strata.extract([ 'a' ]), 'a', 'extract')
            destructible.destroy()
        })
        await destructible.promise

        pages.purge(0)
        okay(pages.size, 0, 'cache empty')

        const vivified = await utilities.vivify(directory)
        okay(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': []
        }, 'created')
    }

    {
        const destructible = new Destructible('create.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile })
            destructible.destroy()
        })
        await destructible.promise
    }
})
