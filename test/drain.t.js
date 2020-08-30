require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('drain.t')

    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'drain')
    await utilities.reset(directory)
    const leaf = utilities.alphabet(3, 3).slice(0, 19)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': leaf.slice(1).map((word, index) => {
            return [ 'insert', index, word ]
        })
    })

    {
        const cache = new Cache
        const strata = new Strata(destructible.ephemeral('split'), { directory, cache })
        await strata.open()
        const cursor = await strata.search(leaf[0])
        const writes = {}
        const { index, found } = cursor.indexOf(leaf[0])
        cursor.insert(index, leaf[0], [ leaf[0] ], writes)
        cursor.release()
        Strata.flush(writes)
        await strata.close()
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged')
    }
    {
        const cache = new Cache
        const strata = new Strata(destructible.ephemeral('reopen'), { directory, cache })
        await strata.open()
        const cursor = await strata.search(leaf[0])
        const { index, found } = cursor.indexOf(leaf[0])
        okay(cursor.page.items[index].parts[0], leaf[0], 'found')
        cursor.release()
        await strata.close()
    }
    {
        const cache = new Cache
        const strata = new Strata(destructible.ephemeral('traverse'), { directory, cache })
        await strata.open()
        let right = leaf[0]
        const items = []
        do {
            const cursor = await strata.search(right)
            const { index, found } = cursor.indexOf(right)
            for (let i = index; i < cursor.page.items.length; i++) {
                items.push(cursor.page.items[i].parts[0])
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, leaf, 'traverse')
        await strata.close()
    }

    // TODO Destruction is off...
    destructible.destroy()
    await destructible.destructed
})
