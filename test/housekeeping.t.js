// This should test a race where a page is queued for housekeeping because it
// has reached the split boundary but when housekeeping queue begins work the
// page has shrunk and is no longer ready to split.

//
require('proof')(5, async (okay) => {
    const Trampoline = require('reciprocate')
    const Strata = require('..')

    const test = require('./test')

    for await (const harness of test('create', okay)) {
        await harness($ => $(), 'open', async ({ strata, prefix }) => {
            const items = []
            const trampoline = new Trampoline
            strata.search(trampoline, 'e', cursor => {
                cursor.insert(cursor.index, 'e', [ 'e' ])
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await null
            strata.search(trampoline, 'e', cursor => {
                cursor.remove(cursor.index)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            okay('true')
        }, {
            serialize: {
                '0.0': [[ '0.1', null ]],
                '0.1': [[ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ], [ 'insert', 3, 'd' ]]
            }
        })
    }
})
