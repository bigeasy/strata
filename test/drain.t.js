require('proof')(2, async (okay) => {
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('./utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'drain')
    await utilities.reset(directory)
    const leaf = utilities.alphabet(3, 3).slice(0, 19)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[ 'right', null ]].concat(leaf.slice(1).map((word, index) => {
            return [ 'insert', index, word ]
        }))
    })
    async function split () {
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const cursor = (await strata.search(leaf[0])).get()
        cursor.insert(leaf[0], leaf[0], cursor.index)
        cursor.release()
        await cursor.flush()
        // TODO Must wait for housekeeping to finish before closing.
        await new Promise(resolve => setTimeout(resolve, 500))
        await strata.close()
        cache.purge(0)
        // **TODO** broken test...
        // okay(cache.heft, 0, 'cache purged')
    }
    await split()
    async function reopen () {
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        const cursor = (await strata.search(leaf[0])).get()
        okay(cursor.items[cursor.index].value, leaf[0], 'found')
        cursor.release()
        await strata.close()
    }
    await reopen()
    async function traverse () {
        const cache = new Cache
        const strata = new Strata({ directory, cache })
        await strata.open()
        let right = leaf[0]
        const items = []
        do {
            const cursor = (await strata.search(right)).get()
            for (let i = cursor.index; i < cursor.items.length; i++) {
                items.push(cursor.items[i].value)
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, leaf, 'traverse')
        await strata.close()
    }
    await traverse()
})
