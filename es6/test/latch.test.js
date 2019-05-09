describe('latch', () => {
    const Latch = require('../latch')
    it('can block and unlatch', async () => {
        const one = new Latch, two = new Latch
        ; (async () => {
            await one.promise
            two.unlatch()
        })()
        one.unlatch()
        await two.promise
    })
})
