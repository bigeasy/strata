require('proof')(5, async (okay) => {
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
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        const writes = {}
        const cursor = await strata.search('f')
        const { index } = cursor.indexOf('f')
        cursor.insert(index, 'f', [ 'f' ], writes)
        cursor.release()
        await Strata.flush(writes)
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    {
        const destructible = new Destructible([ 'split.t', 'reopen' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await Destructible.rescue(async function () {
            await strata.open()
            const cursor = await strata.search('f')
            const { index } = cursor.indexOf('f')
            okay(cursor.page.items[index].parts[0], 'f', 'found')
            cursor.release()
        })
        await strata.destructible.destroy().rejected
    }
    {
        const destructible = new Destructible([ 'split.t', 'traverse' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        let right = 'a'
        const items = []
        do {
            const cursor = await strata.search(right)
            const { index } = cursor.indexOf(right)
            for (let i = index; i < cursor.page.items.length; i++) {
                items.push(cursor.page.items[i].parts[0])
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'traverse')
        await strata.destructible.destroy().rejected
    }
    {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        let right = Strata.MIN
        const items = []
        do {
            const cursor = await strata.search(right)
            const { index } = cursor.indexOf(right)
            for (let i = index; i < cursor.page.items.length; i++) {
                items.push(cursor.page.items[i].parts[0])
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], 'forward')
        await strata.destructible.destroy().rejected
   }
   {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        let left = Strata.MAX, fork = false, cursor
        const items = []
        do {
            cursor = await strata.search(left, fork)
            for (let i = cursor.page.items.length - 1; i >= 0; i--) {
                items.push(cursor.page.items[i].parts[0])
            }
            cursor.release()
            left = cursor.page.items[0].key
            fork = true
        } while (cursor.page.id != '0.1')
        okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ].reverse(), 'reverse')
        await strata.destructible.destroy().rejected
    }
})
