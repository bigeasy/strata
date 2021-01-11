require('proof')(5, async (okay) => {
    const fs = require('fs').promises
    const path = require('path')

    const test = require('./test')

    for await (const harness of test('create', okay)) {
        await harness($ => $(), 'open', async ({ strata, prefix, directory, pages }) => {
            okay(pages.size, 1, 'cache not empty')
        }, {
            serialize: {
                '0.0': [ [ '0.1', null ] ],
                '0.1': []
            }
        })
    }
})
