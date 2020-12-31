require('proof')(13, async (okay) => {
    const Trampoline = require('reciprocate')
    const Strata = require('../strata')

    const utilities = require('../utilities')

    const leaf = utilities.alphabet(4, 4).slice(0, 33)

    const test = require('./test')

    for await (const harness of test('drain', okay, [ 'fileSystem', 'writeahead' ])) {
        await harness($ => $(), 'split', async ({ strata }) => {
            const trampoline = new Trampoline, writes = {}
            strata.search(trampoline, leaf[0], cursor => {
                cursor.insert(cursor.index, leaf[0], leaf, writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)
            await strata.drain()
        }, {
            serialize: {
                '0.0': [[ '0.1', null ]],
                '0.1': leaf.slice(1).map((word, index) => {
                    return [ 'insert', index, word ]
                })
            }
        })
        await harness($ => $(), 'reopen', async ({ strata, prefix }) => {
            const trampoline = new Trampoline
            strata.search(trampoline, leaf[0], cursor => {
                okay(cursor.page.items[cursor.index].parts[0], leaf[0], `${prefix} found`)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        })
        await harness($ => $(), 'traverse', async ({ strata, prefix }) => {
            let right = leaf[0]
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
            okay(items, leaf, `${prefix} traverse`)
        })
    }
})
