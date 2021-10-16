require('proof')(10, async (okay) => {
    const expected = [ 'a', 'b', 'c', 'd', 'e', 'f', 'h', 'i' ]
    const { Trampoline } = require('reciprocate')
    const Strata = require('..')

    const test = require('./test')

    for await (const harness of test('traverse', okay)) {
        await harness($ => $(), 'open', async ({ strata, prefix, directory, pages }) => {
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
            okay(items, expected, 'forward')
        }, {
            serialize: {
                '0.0': [[ '0.1', null ], [ '1.1', [ 'd' ] ], [ '1.3', [ 'g' ] ]],
                '0.1': [[ 'right', [ 'd' ] ], [ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]],
                '1.1': [[ 'right', [ 'g' ] ], [ 'insert', 0, 'd' ], [ 'insert', 1, 'e' ], [ 'insert', 2, 'f' ]],
                '1.3': [
                    [ 'insert', 0, 'g' ], [ 'insert', 1, 'h' ], [ 'insert', 2, 'i' ], [ 'insert', 3, 'j' ],
                    [ 'delete', 0 ], [ 'delete', 2 ]
                ]
            }
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
                    left = cursor.page.key
                    fork = true
                    id = cursor.page.id
                })
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            } while (id != '0.1')
            okay(items, expected.slice().reverse(), `${prefix} reverse`)
        }, {
        })
    }
})
