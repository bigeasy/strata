require('proof')(3, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

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
        const cache = new Cache
        const strata = await Strata.open(new Destructible('merge'), { directory, cache })
        // TODO Come back and insert an error into `remove`. Then attempt to
        // resolve that error somehow into `flush`. Implies that Turnstile
        // propagates an error. Essentially, how do you get the foreground
        // to surrender when the background has failed. `flush` could be
        // waiting on a promise when the background fails and hang
        // indefinately. Any one error, like a `shutdown` error would stop
        // it.
        const writes = {}
        const trampoline = new Trampoline
        await strata.search(trampoline, 'b', cursor => {
            cursor.remove(cursor.index, writes)
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
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
        strata.search(trampoline, 'c', cursor => {
            okay(cursor.page.items[cursor.index].parts[0], 'c', 'found')
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
        await strata.destructible.destroy().rejected
    }
})
