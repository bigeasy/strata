require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const Trampoline = require('reciprocate')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'delete')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ]]
    })
    {
        const destructible = new Destructible('delete.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile })

            const trampoline = new Trampoline, writes = {}
            strata.search(trampoline, 'a', cursor => cursor.remove(cursor.index, writes))
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)

            destructible.destroy()
        })
        await destructible.rejected
        const vivified = await utilities.vivify(directory)
        okay(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': [
                [ 'insert', 0, 'a' ],
                [ 'insert', 1, 'b' ],
                [ 'insert', 2, 'c' ],
                [ 'delete', 0 ]
            ]
        }, 'inserted')
        cache.purge(0)
        // **TODO** Cache purge broken.
        okay(cache.heft, 0, 'cache purged')
    }

    {
        const destructible = new Destructible('delete.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile })
            let right = 'a'
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
            okay(items, [ 'b', 'c' ], 'traverse')

            destructible.destroy()
        })
        await destructible.rejected
    }
})
