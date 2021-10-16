require('proof')(5, async (okay) => {
    const { Trampoline } = require('reciprocate')
    const Strata = require('../strata')
    const Fracture = require('fracture')

    const test = require('./test')

    for await (const harness of test('insert', okay)) {
        await harness($ => $(), 'insert', async ({ strata }) => {
            const trampoline = new Trampoline, promises = []
            strata.search(trampoline, 'a', cursor => {
                promises.push(cursor.insert(Fracture.stack(), cursor.index, [ 'a' ], [ 'a' ]))
                promises.push(cursor.insert(Fracture.stack(), cursor.indexOf([ 'b' ], cursor.index).index, [ 'b' ], [ 'b' ]))
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            for (const promise of promises) {
                await promise
            }
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
