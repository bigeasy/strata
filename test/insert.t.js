require('proof')(5, async (okay) => {
    const Trampoline = require('reciprocate')
    const Strata = require('../strata')

    const test = require('./test')

    for await (const harness of test('insert', okay)) {
        await harness($ => $(), 'insert', async ({ strata, prefix, directory, pages }) => {
            const writes = {}

            const trampoline = new Trampoline
            strata.search(trampoline, 'a', cursor => {
                cursor.insert(cursor.index, 'a', [ 'a' ], writes)
                cursor.insert(cursor.indexOf('b', cursor.index).index, 'B', [ 'b' ], writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }

            await Strata.flush(writes)
        }, {
            create: true,
            vivify: {
                '0.0': [ [ '0.1', null ] ],
                '0.1': [
                    [ 'insert', 0, 'a' ],
                    [ 'insert', 1, 'b' ]
                ]
            }
        })
    }
})
