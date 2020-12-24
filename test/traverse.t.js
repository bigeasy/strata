require('proof')(2, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ], [ '1.1', 'd' ], [ '1.3', 'g' ]],
        '0.1': [[ 'right', 'd' ], [ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]],
        '1.1': [[ 'right', 'g' ], [ 'insert', 0, 'd' ], [ 'insert', 1, 'e' ], [ 'insert', 2, 'f' ]],
        '1.3': [
            [ 'insert', 0, 'g' ], [ 'insert', 1, 'h' ], [ 'insert', 2, 'i' ], [ 'insert', 3, 'j' ],
            [ 'delete', 0 ], [ 'delete', 2 ]
        ]
    })

    const expected = [ 'a', 'b', 'c', 'd', 'e', 'f', 'h', 'i' ]

    {
        const destructible = new Destructible([ 'traverse.t', 'forward' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            let right = Strata.MIN
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
            okay(items, expected, 'forward')
            destructible.destroy()
        })
        await destructible.rejected
    }
    {
        const destructible = new Destructible([ 'traverse.t', 'reverse' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            let left = Strata.MAX, fork = false, cursor, id
            const items = []
            do {
                const trampoline = new Trampoline
                strata.search(trampoline, left, fork, cursor => {
                    for (let i = cursor.page.items.length - 1; i >= 0; i--) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                    left = cursor.page.key
                    fork = true
                    id = cursor.page.id
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (id != '0.1')
            okay(items, expected.slice().reverse(), 'reverse')
            await destructible.destroy()
        })
        await destructible.rejected
    }
})
