require('proof')(18, (okay) => {
    const Cache = require('../cache')
    // construction
    {
        const cache = new Cache
        okay(cache.heft, 0, 'constructed')
    }
    // basic caching
    {
        const cache = new Cache
        const entry = cache.hold([ 1 ], 1)
        okay(entry.value, 1, 'cached')
        entry.release()
    }
    // heft
    {
        const cache = new Cache
        okay(cache.heft, 0, 'initial cache heft')
        const entry = cache.hold([ 1 ], 1)
        okay(entry.value, 1, 'cached')
        okay(entry.heft, 0, 'initial entry heft')
        entry.heft = 1
        okay(entry.heft, 1, 'updated entry heft')
        okay(cache.heft, 1, 'updated cache heft')
        entry.release()
    }
    // getting
    {
        const cache = new Cache
        okay(cache.heft, 0, 'initial cache heft')
        const first = cache.hold([ 1 ], 1)
        okay(first.value, 1, 'cached')
        first.release()
        const second = cache.hold([ 1 ], 2)
        okay(second.value, 1, 'got cached')
        second.release()
    }
    // removing
    {
        const cache = new Cache
        okay(cache.heft, 0, 'initial cache heft')
        const first = cache.hold([ 1 ], 1)
        okay(first.value, 1, 'cached')
        first.heft = 1
        okay(cache.heft, 1, 'set entry heft')
        first.release()
        const second = cache.hold([ 1 ], 2)
        okay(second.value, 1, 'got cached')
        second.remove()
        okay(cache.heft, 0, 'removed object')
        const third = cache.hold([ 1 ], 2)
        okay(third.value, 2, 'inserted new object')
        third.remove()
    }
    // purging
    {
        const cache = new Cache
        const first = cache.hold([ 1 ], 1)
        first.heft = 1
        const second = cache.hold([ 2 ], 1)
        second.heft = 1
        second.release()
        const third = cache.hold([ 3 ], 1)
        third.heft = 1
        third.release()
        okay(cache.heft, 3, 'cache heft at 3')
        cache.purge(2)
        okay(cache.heft, 2, 'cache heft at 2')
    }
})
