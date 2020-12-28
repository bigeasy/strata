require('proof')(3, async (okay) => {
    const path = require('path')

    const Turnstile = require('turnstile')
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Magazine = require('magazine')

    const utilities = require('../utilities')

    const directory = path.join(utilities.directory, 'insert')
    await utilities.reset(directory)

    const pages = new Magazine
    const handles = new Strata.HandleCache(new Magazine)

    const destructible = new Destructible($ => $(), 5000, 'insert.t')
    const turnstile = new Turnstile(destructible.durable('turnstile'))
    destructible.rescue($ => $(), 'test', async () => {
        const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { turnstile, directory, pages, handles, create: true })

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

        await strata.handles.shrink(0)

        destructible.destroy()
    })

    await destructible.promise

    pages.purge(0)

    okay(handles.magazine.size, 0, 'handles purged')
    okay(pages.heft, 0, 'cache empty')

    const vivified = await utilities.vivify(directory)
    okay(vivified, {
        '0.0': [ [ '0.1', null ] ],
        '0.1': [
            [ 'insert', 0, 'a' ],
            [ 'insert', 1, 'b' ]
        ]
    }, 'inserted')
})
