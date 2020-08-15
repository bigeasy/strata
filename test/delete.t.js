require('proof')(3, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('delete.t')
    const Strata = require('../strata')
    const Cache = require('../cache')
    const utilities = require('../test')
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
    destructible.durable('test', async function () {
        async function remove () {
            const cache = new Cache
            const strata = new Strata(destructible.durable('strata'), { directory, cache })
            await strata.open()
            const cursor = (await strata.search('a')).get()
            cursor.remove(cursor.index)
            await cursor.flush()
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
        await remove()
        async function traverse () {
            const cache = new Cache
            const strata = new Strata(destructible.ephemeral('traverse'), { directory, cache })
            await strata.open()
            let right = 'a'
            const items = []
            do {
                const cursor = (await strata.search(right)).get()
                for (let i = cursor.index; i < cursor.items.length; i++) {
                    items.push(cursor.items[i].value)
                }
                cursor.release()
                right = cursor.page.right
            } while (right != null)
            okay(items, [ 'b', 'c' ], 'traverse')
            await strata.close()
        }
        traverse()
    })
    await destructible.destructed
})
