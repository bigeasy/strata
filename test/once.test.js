describe('once', () => {
    const assert = require('assert')
    const once = require('../once')
    const events = require('events')
    it('can return an event', async () => {
        const ee = new events.EventEmitter
        const p = once(ee, 'event')
        ee.emit('event', 1, 2)
        const [ one, two ] = await p
        assert.deepStrictEqual({ one, two }, { one: 1, two: 2 }, 'once')
    })
    it('can throw an error', async () => {
        const ee = new events.EventEmitter
        const p = once(ee, 'event')
        const test = []
        ee.emit('error', new Error('error'))
        try {
            await p
        } catch (error) {
            test.push(error.message)
        }
        assert.deepStrictEqual(test, [ 'error' ], 'once')
    })
    it('can catch an error', async () => {
        const ee = new events.EventEmitter
        const p = once(ee, 'event', null)
        ee.emit('error', new Error('error'))
        await p
    })
})
