require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('merge.t')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'merge')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ], [ '0.3', 'd' ]],
        '0.1': [[
            'right', '0.3'
        ], [
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ]],
        '0.3': [[
            'right', null
        ], [
            'insert', 0, 'd'
        ], [
            'insert', 1, 'e'
        ]]
    })

    destructible.durable('test', async function () {
        async function merge () {
            const cache = new Cache
            const strata = new Strata(destructible.ephemeral('merge'), { directory, cache })
            await strata.open()
            const cursor = (await strata.search('e')).get()
            // TODO Come back and insert an error into `remove`. Then attempt to
            // resolve that error somehow into `flush`. Implies that Turnstile
            // propagates an error. Essentially, how do you get the foreground
            // to surrender when the background has failed. `flush` could be
            // waiting on a promise when the background fails and hang
            // indefinately. Any one error, like a `shutdown` error would stop
            // it.
            cursor.remove(cursor.index)
            cursor.release()
            await cursor.flush()
            await strata.close()
            cache.purge(0)
            okay(cache.heft, 0, 'cache purged')
        }
        await merge()
        async function reopen () {
            const cache = new Cache
            const strata = new Strata(destructible.ephemeral('reopen'), { directory, cache })
            await strata.open()
            const cursor = (await strata.search('d')).get()
            okay(cursor.items[cursor.index].value, 'd', 'found')
            cursor.release()
            await strata.close()
        }
        await reopen()
        async function traverse () {
            const cache = new Cache
            const strata = new Strata(destructible.ephemeral('traverse'), { directory, cache })
            await strata.open()
            let right = 'a'
            const items = []
            do {
                const cursor = (await strata.search(right)).get()
                for (let i = cursor.index; i < cursor.page.items.length; i++) {
                    items.push(cursor.page.items[i].value)
                }
                cursor.release()
                right = cursor.page.right
            } while (right != null)
            okay(items, [ 'a', 'b', 'c', 'd' ], 'traverse')
            await strata.close()
        }
        await traverse()
        destructible.destroy()
    })

    await destructible.destructed
})
