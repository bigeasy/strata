require('proof')(2, async (okay) => {
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ], [ '1.1', 'd' ], [ '1.3', 'g' ]],
        '0.1': [[ 'right', 'd' ], [ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]],
        '1.1': [[ 'right', 'g' ], [ 'insert', 0, 'd' ], [ 'insert', 1, 'e' ], [ 'insert', 2, 'f' ]],
        '1.3': [
            [ 'insert', 0, 'g' ], [ 'insert', 1, 'h' ], [ 'insert', 2, 'i' ], [ 'insert', 3, 'j' ],
            [ 'delete', 0 ], [ 'delete', 3 ]
        ]
    })

    const expected = [ 'a', 'b', 'c', 'd', 'e', 'f', 'h', 'i' ]

    await async function () {
        const destructible = new Destructible([ 'split.t', 'forward' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        let right = Strata.MIN
        const items = []
        do {
            const cursor = (await strata.search(right)).get()
            for (let i = cursor.index; i < cursor.page.items.length; i++) {
                items.push(cursor.page.items[i].value)
            }
            cursor.release()
            right = cursor.page.right
        } while (right != null)
        okay(items, expected, 'forward')
        await strata.close()
        await destructible.destructed
    } ()
    await async function () {
        const destructible = new Destructible([ 'split.t', 'reverse' ])
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        let left = Strata.MAX, fork = false, cursor
        const items = []
        do {
            cursor = (await strata.search(left, fork)).get()
            for (let i = cursor.index; i >= cursor.ghosts; i--) {
                items.push(cursor.page.items[i].value)
            }
            cursor.release()
            left = cursor.page.items[0].key
            console.log(left)
            fork = true
        } while (cursor.page.id != '0.1')
        okay(items, expected.slice().reverse(), 'reverse')
        await strata.close()
        await destructible.destructed
    } ()
})
