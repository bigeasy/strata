require('proof')(15, async (okay) => {
    const Fracture = require('fracture')
    const { Trampoline } = require('reciprocate')
    const Strata = require('../strata')

    const utilities = require('../utilities')

    const test = require('./test')

    for await (const harness of test('fill', okay, [ 'fileSystem', 'writeahead' ])) {
        await harness($ => $(), 'merge', async ({ strata }) => {
            const trampoline = new Trampoline, promises = []
            await strata.search(trampoline, [ 'b' ], cursor => {
                promises.push(cursor.remove(Fracture.stack(), cursor.index))
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            for (const promise of promises) {
                await promise
            }
            await strata.drain()
        }, {
            serialize: {
                '0.0': [[ '0.2', null ], [ '0.4', 'f' ]],
                '0.2': [[ '0.1', null ], [ '0.3', 'c' ]],
                '0.4': [[ '0.5', null ], [ '0.7', 'i' ]],
                '0.1': [[
                    'right', 'c'
                ], [
                    'insert', 0, 'a'
                ], [
                    'insert', 1, 'b'
                ]],
                '0.3': [[
                    'right', 'f'
                ], [
                    'insert', 0, 'c'
                ], [
                    'insert', 1, 'd'
                ], [
                    'insert', 2, 'e'
                ]],
                '0.5': [[
                    'right', 'i'
                ], [
                    'insert', 0, 'f'
                ], [
                    'insert', 1, 'g'
                ], [
                    'insert', 2, 'h'
                ]],
                '0.7': [[
                    'insert', 0, 'i'
                ], [
                    'insert', 1, 'j'
                ], [
                    'insert', 2, 'k'
                ]]
            },
            vivify: {
                '0.0': [[ '0.1', null ], [ '0.5', 'f' ], [ '0.7', 'i' ] ],
                '0.1': [
                    [ 'insert', 0, 'a' ],
                    [ 'insert', 1, 'c' ],
                    [ 'insert', 2, 'd' ],
                    [ 'insert', 3, 'e' ],
                    [ 'right', 'f' ]
                ],
                '0.5': [
                    [ 'insert', 0, 'f' ],
                    [ 'insert', 1, 'g' ],
                    [ 'insert', 2, 'h' ],
                    [ 'right', 'i' ]
                ],
                '0.7': [
                    [ 'insert', 0, 'i' ],
                    [ 'insert', 1, 'j' ],
                    [ 'insert', 2, 'k' ]
                ]
            }
        })
        await harness($ => $(), 'reopen', async ({ strata, prefix }) => {
            const trampoline = new Trampoline
            strata.search(trampoline, [ 'c' ], cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'c', `${prefix} found`)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        })
        await harness($ => $(), 'traverse', async ({ strata, prefix }) => {
            let right = [ 'a' ]
            const items = [], trampoline = new Trampoline
            do {
                strata.search(trampoline, right, cursor => {
                    for (let i = cursor.index; i < cursor.page.items.length; i++) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                    right = cursor.page.right
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (right != null)
            okay(items, [ 'a', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k' ], `${prefix} traverse`)
        })
    }
})
