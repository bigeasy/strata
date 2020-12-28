require('proof')(5, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Magazine = require('magazine')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ], [
            'insert', 3, 'd'
        ], [
            'insert', 4, 'e'
        ]]
    })

    {
        const destructible = new Destructible(5000, [ 'split.t', 'split' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })

            const trampoline = new Trampoline, writes = {}
            strata.search(trampoline, 'f', cursor => {
                cursor.insert(cursor.index, 'f', [ 'f' ], writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)

            await strata.drain()

            destructible.destroy()
        })
        await destructible.promise
        pages.purge(0)
        handles.shrink(0)
        okay(pages.heft, 0, 'pages purged')
    }
    {
        const destructible = new Destructible([ 'split.t', 'reopen' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })

            const trampoline = new Trampoline
            strata.search(trampoline, 'f', cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'f', 'found')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }

            destructible.destroy()
        })
        await destructible.promise
    }
    {
        const destructible = new Destructible([ 'split.t', 'traverse' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        const handles = new Strata.HandleCache(new Magazine)
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, handles, turnstile  })
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
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'traverse')

            destructible.destroy()
        })
        await destructible.promise
    }
    {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, turnstile  })
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
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'forward')
            destructible.destroy()
        })
        await destructible.promise
   }
   {
        const destructible = new Destructible([ 'split.t', 'reverse' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const pages = new Magazine
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, pages, turnstile  })
            let left = Strata.MAX, fork = false, cursor, id
            const items = []
            do {
                const trampoline = new Trampoline
                strata.search(trampoline, left, fork, cursor => {
                    for (let i = cursor.page.items.length - 1; i >= 0; i--) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                    left = cursor.page.items[0].key
                    fork = true
                    id = cursor.page.id
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (id != '0.1')
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ].reverse(), 'reverse')
            destructible.destroy()
        })
        await destructible.promise
    }
})
