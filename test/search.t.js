require('proof')(11, async (okay) => {
    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')

    const Strata = require('../strata')
    const Cache = require('magazine')

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
    const destructible = new Destructible('search.t')
    const turnstile = new Turnstile(destructible.durable($ => $(), 'turnstile'))
    const cache = new Cache
    destructible.rescue($ => $(), 'test', async () => {
        const strata = await Strata.open(destructible.durable($ => $(), 'strata'), { directory, cache, turnstile })
        {
            const trampoline = new Trampoline
            strata.search(trampoline, Strata.MIN, false, cursor => {
                okay(cursor.page.id, '0.1', 'min external')
                okay(cursor.index, 0, 'index set')
                okay(!cursor.found, 'min not found')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, Strata.MAX, cursor => {
                okay(cursor.page.id, '1.3', 'max')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'd', cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'd', true, cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'e', true, cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'j', true, cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'g', true, cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'h', cursor => {
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
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        {
            const trampoline = new Trampoline
            strata.search(trampoline, 'i', true, cursor => {
                okay({
                    id: cursor.page.id,
                    index: cursor.index,
                    found: cursor.found
                }, {
                    id: '1.3',
                    index: -1,
                    found: false
                }, 'fork at zero index')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }
        destructible.destroy()
    })
    await destructible.promise
})
