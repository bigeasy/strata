// This should test a race where a page is queued for housekeeping because it
// has reached the split boundary but when housekeeping queue begins work the
// page has shrunk and is no longer ready to split.

//
require('proof')(1, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Cache = require('magazine')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ], [ 'insert', 3, 'd' ]]
    })

    {
        const destructible = new Destructible('housekeeping.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            const items = []
            const trampoline = new Trampoline
            strata.search(trampoline, 'e', cursor => {
                cursor.insert(cursor.index, 'e', [ 'e' ], {})
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await null
            strata.search(trampoline, 'e', cursor => {
                cursor.remove(cursor.index, {})
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay('true')
            destructible.destroy()
        })
        await destructible.promise
    }
})
