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
        '0.0': [[ '0.1', null ], [ '1.1', 'd' ], [ '1.3', 'g' ]],
        '0.1': [[ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]]
    })

    {
        const destructible = new Destructible('load.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            let right = Strata.MIN
            const items = []
            function search () {
                const trampoline = new Trampoline
                strata.search(trampoline, Strata.MIN, cursor => {
                    for (let i = cursor.index; i < cursor.page.items.length; i++) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                })
                return trampoline
            }
            const trampolines = []
            trampolines.push(search())
            trampolines.push(search())
            for (const trampoline of trampolines) {
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(items, [ 'a', 'b', 'c', 'a', 'b', 'c' ], 'raceed')
            destructible.destroy()
        })
        await destructible.rejected
    }
})
