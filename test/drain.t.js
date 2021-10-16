require('proof')(15, async (okay) => {
    const { Trampoline } = require('reciprocate')
    const Strata = require('../strata')
    const Fracture = require('fracture')

    const utilities = require('../utilities')

    const leaf = utilities.alphabet(4, 4).slice(0, 33)

    const test = require('./test')

    for await (const harness of test('drain', okay)) {
        await harness($ => $(), 'split', async ({ strata }) => {
            const trampoline = new Trampoline, promises = []
            strata.search(trampoline, leaf[0], cursor => {
                promises.push(cursor.insert(Fracture.stack(), cursor.index, leaf[0], leaf))
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
                '0.0': [[ '0.1', null ]],
                '0.1': leaf.slice(1).map((word, index) => {
                    return [ 'insert', index, word ]
                })
            },
            vivify: {
               '0.0': [
                 [ '1.8', null ],
                 [ '1.10', 'aaca' ],
                 [ '1.14', 'abaa' ],
                 [ '1.18', 'abca' ]
               ],
               '1.8': [ [ '0.1', null ], [ '1.7', 'aaba' ] ],
               '0.1': [
                 [ 'insert', 0, 'aaaa' ],
                 [ 'insert', 1, 'aaab' ],
                 [ 'insert', 2, 'aaac' ],
                 [ 'insert', 3, 'aaad' ],
                 [ 'right', 'aaba' ]
               ],
               '1.7': [
                 [ 'insert', 0, 'aaba' ],
                 [ 'insert', 1, 'aabb' ],
                 [ 'insert', 2, 'aabc' ],
                 [ 'insert', 3, 'aabd' ],
                 [ 'right', 'aaca' ]
               ],
               '1.10': [ [ '1.3', null ], [ '1.11', 'aada' ] ],
               '1.3': [
                 [ 'insert', 0, 'aaca' ],
                 [ 'insert', 1, 'aacb' ],
                 [ 'insert', 2, 'aacc' ],
                 [ 'insert', 3, 'aacd' ],
                 [ 'right', 'aada' ]
               ],
               '1.11': [
                 [ 'insert', 0, 'aada' ],
                 [ 'insert', 1, 'aadb' ],
                 [ 'insert', 2, 'aadc' ],
                 [ 'insert', 3, 'aadd' ],
                 [ 'right', 'abaa' ]
               ],
               '1.14': [ [ '1.1', null ], [ '1.13', 'abba' ] ],
               '1.1': [
                 [ 'insert', 0, 'abaa' ],
                 [ 'insert', 1, 'abab' ],
                 [ 'insert', 2, 'abac' ],
                 [ 'insert', 3, 'abad' ],
                 [ 'right', 'abba' ]
               ],
               '1.13': [
                 [ 'insert', 0, 'abba' ],
                 [ 'insert', 1, 'abbb' ],
                 [ 'insert', 2, 'abbc' ],
                 [ 'insert', 3, 'abbd' ],
                 [ 'right', 'abca' ]
               ],
               '1.18': [ [ '1.5', null ], [ '1.15', 'abda' ], [ '1.17', 'abdc' ] ],
               '1.5': [
                 [ 'insert', 0, 'abca' ],
                 [ 'insert', 1, 'abcb' ],
                 [ 'insert', 2, 'abcc' ],
                 [ 'insert', 3, 'abcd' ],
                 [ 'right', 'abda' ]
               ],
               '1.15': [
                 [ 'insert', 0, 'abda' ],
                 [ 'insert', 1, 'abdb' ],
                 [ 'right', 'abdc' ]
               ],
               '1.17': [
                 [ 'insert', 0, 'abdc' ],
                 [ 'insert', 1, 'abdd' ],
                 [ 'insert', 2, 'acaa' ]
               ]
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
