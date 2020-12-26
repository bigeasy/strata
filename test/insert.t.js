require('proof')(2, async (okay) => {
    const path = require('path')

    const Turnstile = require('turnstile')
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('magazine')

    const utilities = require('../utilities')

    const directory = path.join(utilities.directory, 'insert')
    await utilities.reset(directory)

    const cache = new Cache

    const destructible = new Destructible($ => $(), 5000, 'insert.t')
    const turnstile = new Turnstile(destructible.durable('turnstile'))
    destructible.rescue($ => $(), 'test', async () => {
        const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { turnstile, directory, cache, create: true })

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

        destructible.destroy()
    })

    await destructible.promise

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
