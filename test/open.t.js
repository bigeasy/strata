require('proof')(7, async (okay) => {
    const fs = require('fs').promises
    const path = require('path')

    const test = require('./test')

    for await (const harness of test('create', okay)) {
        await harness($ => $(), 'open', async ({ strata, prefix, directory, pages }) => {
            console.log(directory)
            const instances = await fs.readdir(path.join(directory, 'instances'))
            okay(instances, [ '1' ], 'instance')
            okay(pages.size, 1, 'cache not empty')
        }, {
            serialize: {
                '0.0': [ [ '0.1', null ] ],
                '0.1': []
            }
        })
    }
})
