require('proof')(12, async (okay) => {
    const test = require('./test')

    for await (const harness of test('create', okay)) {
        await harness($ => $(), 'create', async ({ strata, prefix }) => {
            okay(strata.options.comparator.leaf('a', 'a'), 0, `${prefix} compare`)
            okay(strata.storage.extractor([ 'a' ]), 'a', `${prefix} extract`)
        }, {
            create: true,
            vivify: {
                '0.0': [ [ '0.1', null ] ],
                '0.1': []
            }
        })
        await harness($ => $(), 'reopen', async (strata, prefix) => {})
    }
})
