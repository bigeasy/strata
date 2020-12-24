require('proof')(3, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Cache = require('magazine')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'fill')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.2', null ], [ '0.4', 'f' ]],
        '0.2': [[ '0.1', null ], [ '0.3', 'c' ]],
        '0.4': [[ '0.5', null ], [ '0.7', 'i' ]],
        '0.1': [[
            'right', 'c'
        ], [
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ]],
        '0.3': [[
            'right', 'f'
        ], [
            'insert', 0, 'c'
        ], [
            'insert', 1, 'd'
        ], [
            'insert', 2, 'e'
        ]],
        '0.5': [[
            'right', 'i'
        ], [
            'insert', 0, 'f'
        ], [
            'insert', 1, 'g'
        ], [
            'insert', 2, 'h'
        ]],
        '0.7': [[
            'insert', 0, 'i'
        ], [
            'insert', 1, 'j'
        ], [
            'insert', 2, 'k'
        ]]
    })

    // Merge
    {
        const destructible = new Destructible('fill.t')
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            // TODO Come back and insert an error into `remove`. Then attempt to
            // resolve that error somehow into `flush`. Implies that Turnstile
            // propagates an error. Essentially, how do you get the foreground
            // to surrender when the background has failed. `flush` could be
            // waiting on a promise when the background fails and hang
            // indefinately. Any one error, like a `shutdown` error would stop
            // it.
            const trampoline = new Trampoline, writes = {}
            await strata.search(trampoline, 'b', cursor => {
                cursor.remove(cursor.index, writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)
            destructible.destroy()
        })
        await destructible.rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    // Reopen.
    {
        const destructible = new Destructible([ 'fill.t', 'reopen' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            const trampoline = new Trampoline
            strata.search(trampoline, 'c', cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'c', 'found')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            destructible.destroy()
        })
        await destructible.rejected
    }
    // Traverse.
    {
        const destructible = new Destructible([ 'fill.t', 'traverse' ])
        const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
        const cache = new Cache
        destructible.rescue($ => $(), 'test', async () => {
            const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile  })
            let right = 'a'
            const items = [], trampoline = new Trampoline
            do {
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
            okay(items, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k' ], 'traverse')
            destructible.destroy()
        })
        await destructible.rejected
    }
})
