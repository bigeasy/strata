// Strata expects you to check that the page you've received from
// `Strata.search` has not changed in the time it took for you to resolve the
// `Promise` returned by search. By changed we mean that that the structure of
// the b-tree has not changed — the tree has not been rebalanced. Your search is
// in a race against Strata's balancer, which may decide to split the page, so
// that the key you sought is not on a different page, or merge the page so that
// the page your looking at is now deleted.
//
// Strata asks that you always use `Cursor.indexOf` to find the record, even
// after a return from `search`. `Cursor.indexOf` will return the index for the
// key in the page or `null` if the page is no longer the correct page for the
// key. The `search` method only searches for the page where the key belongs,
// where it should be.  When strata descends the tree, it doesn't actually
// perform the final binary search in the leaf page, it leaves that up to you.
//
// Unit tests for  will be impossible without the means to delay the return from
// `Strata.search`, so we've created `Racer`, a wrapper around `Strata` that
// will let you test your defenses against race conditions. Fortunately, this is
// all you need to test for race conditions. Your code should retry if
// `Cursor.indexOf` returns null, so your tests can delay the return of a search
// for a key, then split or merge the page so that `Cursor.indexOf` return
// `null`.
//
// A reminder that Strata is a b-tree primitive. It's not meant to be a
// database, but a building block for databases.

//
const Strata = require('./strata')

class Racer {
    static MIN = Strata.MIN

    static MAX = Strata.MAX

    constructor (strata, selector) {
        this._strata = strata
        this._selector = selector
        this._count = 0
        this._latch = { promise: null, resolve: () => {} }
        this._events = []
    }

    [Symbol.asyncIterator] () {
        return this
    }

    async next () {
        for (;;) {
            if (this._events.length == 0 || this._done) {
                if (this._done) {
                    return { done: true, value: null }
                }
                this._latch = { promise: null, resolve: null }
                this._latch.promise = new Promise(resolve => this._latch.resolve = resolve)
                await this._latch.promise
                continue
            }
            return { done: false, value: this._events.shift() }
        }
    }

    create () {
        return this._strata.create()
    }

    drain () {
        // TODO Strata needs a drain which is useful only for testing.
    }

    open () {
        return this._strata.open()
    }

    static nullCursor = Strata.nullCursor

    async search (key, fork = false) {
        const cursor = await this._strata.search(key, fork)
        const entry = { count: this._count++, key, fork, cursor }
        const context = this._selector(entry)
        if (context != null) {
            const promise = new Promise(resolve => entry.resolve = () => resolve())
            this._events.push({ context, ...entry })
            this._latch.resolve()
            await promise
        }
        return cursor
    }

    static flush = Strata.flush

    async close () {
        await this._strata.close()
        this._done = true
        this._latch.resolve()
    }
}

module.exports = Racer
