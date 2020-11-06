require('proof')(5, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

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
        const destructible = new Destructible([ 'split.t', 'split' ])
        const cache = new Cache
        const strata = await Strata.open(destructible, { directory, cache })
        const writes = {}
        const trampoline = new Trampoline
        strata.search(trampoline, 'f', cursor => {
            cursor.insert(cursor.index, 'f', [ 'f' ], writes)
        })
        while (trampoline.seek()) {
            await trampoline.shift()
        }
        await Strata.flush(writes)
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    {
        const destructible = new Destructible([ 'split.t', 'reopen' ])
        const cache = new Cache
        const strata = await Strata.open(destructible, { directory, cache })
        await Destructible.rescue(async function () {
            const trampoline = new Trampoline
            strata.search(trampoline, 'f', cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'f', 'found')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        })
        await strata.destructible.destroy().rejected
    }
    {
        const destructible = new Destructible([ 'split.t', 'traverse' ])
        const cache = new Cache
        const strata = await Strata.open(destructible, { directory, cache })
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
        await strata.destructible.destroy().rejected
    }
    {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const cache = new Cache
        const strata = await Strata.open(destructible, { directory, cache })
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
        await strata.destructible.destroy().rejected
   }
   {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const cache = new Cache
        const strata = await Strata.open(destructible, { directory, cache })
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
        await strata.destructible.destroy().rejected
    }
})
