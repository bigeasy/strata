require('proof')(13, async (okay) => {
    const Trampoline = require('reciprocate')
    const Fracture = require('fracture')
    const Strata = require('../strata')

    const test = require('./test')

    for await (const harness of test('merge', okay, [ 'fileSystem', 'writeahead' ])) {
        await harness($ => $(), 'merge', async ({ strata, prefix }) => {
            const trampoline = new Trampoline, promises = []
            strata.search(trampoline, 'e', cursor => {
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
                '0.0': [[ '0.1', null ], [ '0.3', 'd' ]],
                '0.1': [[
                    'right', '0.3'
                ], [
                    'insert', 0, 'a'
                ], [
                    'insert', 1, 'b'
                ], [
                    'insert', 2, 'c'
                ]],
                '0.3': [[
                    'insert', 0, 'd'
                ], [
                    'insert', 1, 'e'
                ]]
            }
        })
        await harness($ => $(), 'reopen', async ({ strata, prefix }) => {
            const trampoline = new Trampoline
            strata.search(trampoline, 'd', cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'd', `${prefix} found`)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        })
        await harness($ => $(), 'traverse', async ({ strata, prefix }) => {
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
            okay(items, [ 'a', 'b', 'c', 'd' ], `${prefix} traverse`)
        })
    }
})
