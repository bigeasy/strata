require('proof')(8, async (okay) => {
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
            [ 'delete', 0 ], [ 'delete', 3 ]
        ]
    })

    const expected = [ 'a', 'b', 'c', 'd', 'e', 'f', 'h', 'i' ]

    // TODO Inserting a ghost... Do you undelete or do you insert a new record?
    await async function () {
        const destructible = new Destructible('search.t')
        const cache = new Cache
        const strata = new Strata(destructible, { directory, cache })
        await strata.open()
        {
            const cursor = (await strata.search(Strata.MIN)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '0.1',
                index: 0
            }, 'min')
            cursor.release()
        }
        {
            const cursor = (await strata.search(Strata.MAX)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '1.3',
                index: 2
            }, 'max')
            cursor.release()
        }
        {
            const cursor = (await strata.search('d')).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            },  {
                id: '1.1',
                index: 0
            }, 'find')
            cursor.release()
        }
        {
            const cursor = (await strata.search('d', true)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '0.1',
                index: 2
            }, 'fork')
            cursor.release()
        }
        {
            const cursor = (await strata.search('e', true)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '1.1',
                index: 0
            }, 'approximate fork')
            cursor.release()
        }
        {
            const cursor = (await strata.search('j', true)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '1.3',
                index: 1
            }, 'approximate fork missing')
            cursor.release()
        }
        {
            const cursor = (await strata.search('g', true)).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '1.1',
                index: 2
            }, 'approximate fork missing end of page')
            cursor.release()
        }
        {
            const cursor = (await strata.search('h')).get()
            okay({
                id: cursor.page.id,
                index: cursor.index
            }, {
                id: '1.3',
                index: 1
            }, 'ghost')
            cursor.release()
        }
        await strata.close()
        await destructible.destructed
    } ()
})
