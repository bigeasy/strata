describe('callback', () => {
    const assert = require('assert')
    const callback = require('../callback')
    it('can resolve a callback', async () => {
        const [ one, two ] = await callback((callback) => callback(null, 1, 2))
        assert.deepStrictEqual({ one, two }, { one: 1, two: 2 }, 'resolve')
    })
    it('can throw an exception', async () => {
        const test = []
        try {
            await callback((callback) => callback(new Error('error')))
        } catch (error) {
            test.push(error.message)
        }
        assert.deepStrictEqual(test, [ 'error' ], 'resolve')
    })
})
