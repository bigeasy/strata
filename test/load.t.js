require('proof')(5, async (okay) => {
    const Trampoline = require('reciprocate')
    const Strata = require('..')

    const test = require('./test')

    for await (const harness of test('create', okay)) {
        await harness($ => $(), 'open', async ({ strata, prefix, directory, pages }) => {
            let right = Strata.MIN
            const items = []
            function search () {
                const trampoline = new Trampoline
                strata.search(trampoline, Strata.MIN, cursor => {
                    for (let i = cursor.index; i < cursor.page.items.length; i++) {
                        items.push(cursor.page.items[i].parts[0])
                    }
                })
                return trampoline
            }
            const trampolines = []
            trampolines.push(search())
            trampolines.push(search())
            for (const trampoline of trampolines) {
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
            }
            okay(items, [ 'a', 'b', 'c', 'a', 'b', 'c' ], 'raced')
        }, {
            serialize: {
                '0.0': [[ '0.1', null ], [ '1.1', 'd' ], [ '1.3', 'g' ]],
                '0.1': [[ 'insert', 0, 'a' ], [ 'insert', 1, 'b' ], [ 'insert', 2, 'c' ]]
            }
        })
    }
})
