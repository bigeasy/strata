require('proof')(3, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')
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
            'insert', 0, 'd'
        ], [
            'insert', 1, 'e'
        ]]
    })

    // Merge.
    {
        const cache = new Cache
        const strata = await Strata.open(new Destructible('merge'), { directory, cache })
        const writes = {}
        const trampoline = new Trampoline
        strata.search(trampoline, 'e', cursor => {
            cursor.remove(cursor.index, writes)
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        // TODO Come back and insert an error into `remove`. Then attempt to
        // resolve that error somehow into `flush`. Implies that Turnstile
        // propagates an error. Essentially, how do you get the foreground to
        // surrender when the background has failed. `flush` could be waiting on
        // a promise when the background fails and hang indefinately. Any one
        // error, like a `shutdown` error would stop it.
        await Strata.flush(writes)
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    // Reopen.
    {
        const cache = new Cache
        const strata = await Strata.open(new Destructible('reopen'), { directory, cache })
        const trampoline = new Trampoline
        strata.search(trampoline, 'd', cursor => {
            console.log(cursor.index, cursor.page.id, cursor.page.items)
            okay(cursor.page.items[cursor.index].parts[0], 'd', 'found')
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        await strata.destructible.destroy().rejected
    }
    // Traverse.
    {
        const cache = new Cache
        const strata = await Strata.open(new Destructible('traverse'), { directory, cache })
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
        okay(items, [ 'a', 'b', 'c', 'd' ], 'traverse')
        await strata.destructible.destroy().rejected
    }
})
