require('proof')(11, async (okay) => {
    const Destructible = require('destructible')

    const Strata = require('../strata')
    const Cache = require('../cache')

    const utilities = require('../utilities')
    const path = require('path')
    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ], [ '1.1', 'd' ], [ '1.3', 'h' ]],
        '0.1': [[ 'right', 'd' ], [ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]],
        '1.1': [[ 'right', 'h' ], [ 'insert', 0, 'd' ], [ 'insert', 1, 'e' ], [ 'insert', 2, 'f' ]],
        '1.3': [
            [ 'insert', 0, 'h' ], [ 'insert', 1, 'i' ], [ 'insert', 2, 'k' ], [ 'insert', 3, 'l' ],
            [ 'delete', 0 ], [ 'delete', 2 ]
        ]
    })

    const expected = [ 'a', 'b', 'c', 'd', 'e', 'f', 'h', 'i' ]

    // TODO What to do if we approimate fork the first item in the tree?
    // TODO Does approimate fork of last item of tree work as expected?
    // TODO Does approimate fork of item past end of tree work?
    // Options are `-1` or `null`.
    {
        const destructible = new Destructible('search.t')
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        {
            const promises = strata.search(Strata.MIN, false, cursor => {
                okay(cursor.page.id, '0.1', 'min external')
                okay(cursor.index, 0, 'index set')
                okay(!cursor.found, 'min not found')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search(Strata.MAX, cursor => {
                okay(cursor.page.id, '1.3', 'max')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('d', cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found
                },  {
                    id: '1.1',
                    index: 0,
                    found: true
                }, 'find')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('d', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found
                }, {
                    id: '0.1',
                    index: 2,
                    found: false
                }, 'fork')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('e', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found,
                    item: cursor.page.items[cursor.index].key
                }, {
                    id: '1.1',
                    index: 0,
                    found: false,
                    item: 'd'
                }, 'fork approimate')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('j', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found,
                    item: cursor.page.items[cursor.index].key
                }, {
                    id: '1.3',
                    index: 0,
                    found: false,
                    item: 'i'
                }, 'approximate fork missing')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('g', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found,
                    item: cursor.page.items[cursor.index].key
                }, {
                    id: '1.1',
                    index: 2,
                    found: false,
                    item: 'f'
                }, 'approximate fork missing end of page')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('h', cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found,
                    item: cursor.page.items[cursor.index].key
                }, {
                    id: '1.3',
                    index: 0,
                    found: false,
                    item: 'i'
                }, 'missing key')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        {
            const promises = strata.search('i', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found
                }, {
                    id: '1.3',
                    index: null,
                    found: false
                }, 'fork at zero index')
            })
            while (promises.length != 0) {
                await promises.shift()
            }
        }
        await strata.destructible.destroy().rejected
    }
})
