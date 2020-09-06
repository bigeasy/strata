require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('delete.t')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'delete')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ]]
    })
    {
        const cache = new Cache
        const strata = new Strata(destructible.ephemeral('strata'), { directory, cache })
        await strata.open()
        const cursor = await strata.search('a')
        const writes = {}
        const { index, found } = cursor.indexOf('a')
        cursor.remove(index, writes)
        Strata.flush(writes)
        cursor.release()
        await strata.close()
        const vivified = await utilities.vivify(directory)
        okay(vivified, {
            '0.0': [ [ '0.1', null ] ],
            '0.1': [
                [ 'insert', 0, 'a' ],
                [ 'insert', 1, 'b' ],
                [ 'insert', 2, 'c' ],
                [ 'delete', 0 ]
            ]
        }, 'inserted')
        cache.purge(0)
        // **TODO** Cache purge broken.
        okay(cache.heft, 0, 'cache purged')
    }
    {
        const cache = new Cache
        const strata = new Strata(destructible.ephemeral('traverse'), { directory, cache })
        await strata.open()
        let right = 'a'
        const items = []
        do {
            const cursor = await strata.search(right)
            const { index, found } = cursor.indexOf('a', cursor.page.ghosts)
            for (let i = index; i < cursor.page.items.length; i++) {
                items.push(cursor.page.items[i].parts[0])
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, [ 'b', 'c' ], 'traverse')
        await strata.close()
    }
    destructible.destroy()
    await destructible.rejected
})
