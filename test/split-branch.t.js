require('proof')(3, async (okay) => {
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split-branch')
    await utilities.reset(directory)
    const leaf = utilities.alphabet(4, 4).slice(0, 33)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': leaf.slice(1).map((word, index) => {
            return [ 'insert', index, word ]
        })
    })

    // Split.
    {
        const cache = new Cache
        const strata = new Strata(new Destructible('split'), { directory, cache })
        await strata.open()
        const writes = {}
        const promises = strata.search(leaf[0], cursor => {
            cursor.insert(cursor.index, leaf[0], leaf, writes)
        })
        while (promises.length != 0) {
            await promises.shift()
        }
        Strata.flush(writes)
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    // Reopen.
    {
        const cache = new Cache
        const strata = new Strata(new Destructible('reopen'), { directory, cache })
        await strata.open()
        const promises = strata.search(leaf[0], cursor => {
            okay(cursor.page.items[cursor.index].parts[0], leaf[0], 'found')
        })
        while (promises.length != 0) {
            await promises.shift()
        }
        await strata.destructible.destroy().rejected
    }
    // Traverse.
    {
        const cache = new Cache
        const strata = new Strata(new Destructible('traverse'), { directory, cache })
        await strata.open()
        let right = leaf[0]
        const items = []
        do {
            const promises = strata.search(right, cursor => {
                for (let i = cursor.index; i < cursor.page.items.length; i++) {
                    items.push(cursor.page.items[i].parts[0])
                }
                right = cursor.page.right
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        } while (right != null)
        okay(items, leaf, 'traverse')
        await strata.destructible.destroy().rejected
    }
})
