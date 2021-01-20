require('proof')(23, async (okay) => {
    const Trampoline = require('reciprocate')
    const Fracture = require('fracture')
    const Strata = require('../strata')

    const test = require('./test')

    for await (const harness of test('split', okay, [ 'fileSystem', 'writeahead' ])) {
        await harness($ => $(), 'split', async ({ strata }) => {
            const trampoline = new Trampoline, writes = new Fracture.FutureSet
            strata.search(trampoline, 'f', cursor => {
                cursor.insert(cursor.index, 'f', [ 'f' ], writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await writes.join()
            await strata.drain()
        }, {
            serialize: {
                '0.0': [[ '0.1', null ]],
                '0.1': [[
                    'insert', 0, 'a'
                ], [
                    'insert', 1, 'b'
                ], [
                    'insert', 2, 'c'
                ], [
                    'insert', 3, 'd'
                ], [
                    'insert', 4, 'e'
                ]]
            }
        })

        await harness($ => $(), 'reopen', async ({ strata, prefix }) => {
            const trampoline = new Trampoline
            strata.search(trampoline, 'f', cursor => {
                okay(cursor.page.items[cursor.index].parts[0], 'f', `${prefix} found`)
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
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], `${prefix} traverse`)
        })

        await harness($ => $(), 'forward', async ({ strata, prefix }) => {
            let right = Strata.MIN
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
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ], `${prefix} forward`)
        })

        await harness($ => $(), 'reverse', async ({ strata, prefix }) => {
            let left = Strata.MAX, fork = false, cursor, id
            const items = []
            do {
                const trampoline = new Trampoline
                strata.search(trampoline, left, fork, cursor => {
                    for (let i = cursor.page.items.length - 1; i >= 0; i--) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                    left = cursor.page.items[0].key
                    fork = true
                    id = cursor.page.id
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (id != '0.1')
            okay(items, [ 'a', 'b', 'c', 'd', 'e', 'f' ].reverse(), `${prefix} reverse`)
        })
    }
})
