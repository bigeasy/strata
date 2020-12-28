require('proof')(4, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Magazine = require('magazine')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'drain')
    await utilities.reset(directory)
    const leaf = utilities.alphabet(3, 3).slice(0, 19)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': leaf.slice(1).map((word, index) => {
            return [ 'insert', index, word ]
        })
    })

    {
        const destructible = new Destructible('drain.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })
            const trampoline = new Trampoline, writes = {}
            strata.search(trampoline, leaf[0], cursor => {
                cursor.insert(cursor.index, leaf[0], [ leaf[0] ], writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)
            destructible.destroy()
        })
        await destructible.promise
        await handles.shrink(0)
        pages.purge(0)
        okay(pages.heft, 0, 'cache purged')
        okay(handles.magazine.heft, 0, 'cache purged')
    }
    {
        const destructible = new Destructible([ 'drain.t', 'reopen' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })
            const trampoline = new Trampoline
            strata.search(trampoline, leaf[0], cursor => {
                okay(cursor.page.items[cursor.index].parts[0], leaf[0], 'found')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            destructible.destroy()
        })
        await destructible.promise
    }
    {
        const destructible = new Destructible([ 'drain.t', 'turnstile' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })
            let right = leaf[0]
            const items = []
            do {
                const trampoline = new Trampoline
                strata.search(trampoline, right, cursor => {
                    for (let i = cursor.index; i < cursor.page.items.length; i++) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                    right = cursor.page.right
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (right != null)
            okay(items, leaf, 'traverse')
            destructible.destroy()
        })
        await destructible.promise
    }
})
