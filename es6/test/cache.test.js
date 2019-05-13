describe('cache', () => {
    const assert = require('assert')
    const Cache = require('../cache')
    it('can be constructed', () => {
        const cache = new Cache
        assert.equal(cache.heft, 0, 'constructed')
    })
    it('can cache an object', () => {
        const cache = new Cache
        const entry = cache.hold([ 1 ], 1)
        assert.equal(entry.value, 1, 'cached')
        entry.release()
    })
    it('can set heft', () => {
        const cache = new Cache
        assert.equal(cache.heft, 0, 'initial cache heft')
        const entry = cache.hold([ 1 ], 1)
        assert.equal(entry.value, 1, 'cached')
        assert.equal(entry.heft, 0, 'initial entry heft')
        entry.heft = 1
        assert.equal(entry.heft, 1, 'updated entry heft')
        assert.equal(cache.heft, 1, 'updated cache heft')
        entry.release()
    })
    it('can get a cached object', () => {
        const cache = new Cache
        assert.equal(cache.heft, 0, 'initial cache heft')
        const first = cache.hold([ 1 ], 1)
        assert.equal(first.value, 1, 'cached')
        first.release()
        const second = cache.hold([ 1 ], 2)
        assert.equal(second.value, 1, 'got cached')
        second.release()
    })
    it('can get remove a cached object', () => {
        const cache = new Cache
        assert.equal(cache.heft, 0, 'initial cache heft')
        const first = cache.hold([ 1 ], 1)
        assert.equal(first.value, 1, 'cached')
        first.heft = 1
        assert.equal(cache.heft, 1, 'set entry heft')
        first.release()
        const second = cache.hold([ 1 ], 2)
        assert.equal(second.value, 1, 'got cached')
        second.remove()
        assert.equal(cache.heft, 0, 'removed object')
        const third = cache.hold([ 1 ], 2)
        assert.equal(third.value, 2, 'inserted new object')
        third.remove()
    })
    it('can purge objects', () => {
        const cache = new Cache
        const first = cache.hold([ 1 ], 1)
        first.heft = 1
        const second = cache.hold([ 2 ], 1)
        second.heft = 1
        second.release()
        const third = cache.hold([ 3 ], 1)
        third.heft = 1
        third.release()
        assert.equal(cache.heft, 3, 'cache heft at 3')
        cache.purge(2)
        assert.equal(cache.heft, 2, 'cache heft at 2')
    })
})
