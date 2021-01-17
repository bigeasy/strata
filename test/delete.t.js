require('proof')(10, async (okay) => {
    const Trampoline = require('reciprocate')
    const Fracture = require('fracture')
    const Strata = require('../strata')

    const test = require('./test')

    for await (const harness of test('delete', okay)) {
        await harness($ => $(), 'delete', async ({ strata, prefix, directory, pages }) => {
            const trampoline = new Trampoline, writes = new Fracture.CompletionSet
            strata.search(trampoline, 'a', cursor => cursor.remove(cursor.index, writes))
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await writes.clear()
        }, {
            serialize: {
                '0.0': [[ '0.1', null ]],
                '0.1': [[
                    'insert', 0, 'a'
                ], [
                    'insert', 1, 'b'
                ], [
                    'insert', 2, 'c'
                ]]
            },
            vivify: {
                '0.0': [ [ '0.1', null ] ],
                '0.1': [
                    [ 'insert', 0, 'b' ],
                    [ 'insert', 1, 'c' ]
                ]
            }
        })
        await harness($ => $(), 'traverse', async ({ strata, prefix, directory, pages }) => {
            let right = 'a'
            const items = []
            do {
                const trampoline = new Trampoline
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
            okay(items, [ 'b', 'c' ], 'traverse')
        })
    }
})
