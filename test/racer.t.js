require('proof')(1, async okay => {
    const Destructible = require('destructible')

    const destructible = new Destructible('racer.t')

    const utilities = require('../utilities')
    const path = require('path')

    const Strata = require('..')
    const Cache = require('../cache')
    const Racer = require('../racer')

    const directory = path.join(utilities.directory, 'racer')
    await utilities.reset(directory)

    const cache = new Cache

    // Test created as well as invoking the initial null latch.
    const created = new Strata(destructible.ephemeral('created'), { directory, cache })
    await created.create()
    await created.close()

    await utilities.reset(directory)
    await utilities.serialize(directory, {
        '0.0': [[ '0.1', null ]],
        '0.1': [[
            'insert', 0, 'a'
        ], [
            'insert', 1, 'b'
        ], [
            'insert', 2, 'c'
        ], [
            'insert', 3, 'd'
        ], [
            'insert', 4, 'e'
        ]]
    })

    // Test actually delaying a query.
    const strata = new Strata(destructible.durable('strata'), { directory, cache })

    const racer = new Racer(strata, function ({ key }) {
        if (key == 'e') {
            return true
        }
    })

    destructible.durable('interfere', async function () {
        for await (const { key, resolve } of racer) {
            okay(key, 'e', 'delayed')
            resolve()
        }
    })

    destructible.durable('descend', async function () {
        await racer.open()
        ; (await racer.search('d')).release()
        ; (await racer.search('e')).release()
        await racer.close()
    })

    await destructible.rejected
})
