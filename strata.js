const Journalist = require('./journalist')
const Cursor = require('./cursor')
const assert = require('assert')

class Strata {
    static MIN = Symbol('MIN')

    static MAX = Symbol('MAX')

    constructor (destructible, options) {
        this.destructible = destructible
        this._journalist = new Journalist(destructible, options)
        const { comparator, extractor }  = this._journalist
        this.compare = function (left, right) { return comparator.leaf(left, right) }
        this.extract = function (parts) { return extractor(parts) }
    }

    create () {
        return this._journalist.create()
    }

    open () {
        return this._journalist.open()
    }

    // What was the lock for? It was to ensure that another strand doesn't
    // change the location of the index between in time it takes return from the
    // async call to `Strata.search`.
    //
    // TODO A race condition occurred to you. What if the page is deleted in
    // during some window and the cursor is invalid, but our descent is itself
    // synchornous, except now we can see below that it isn't, the call to
    // `Journalist.descend` introduces the problem we tried to resolve with our
    // lock, so we ought to move the lock into `Journalist`.

    //
    search (trampoline, key, ...vargs) {
        const [ fork, found ] = vargs.length == 2 ? vargs : [ false, vargs[0] ]
        const query = key === Strata.MIN
            ? { key: null, rightward: false, fork: false }
            : key === Strata.MAX
                ? { key: null, rightward: true, fork: false }
                : { key, rightward: false, fork: fork, approximate: true }
        this._journalist.descend2(trampoline, query, descent => {
            const cursor = new Cursor(this._journalist, descent, key)
            try {
                found(cursor)
            } finally {
                cursor.release()
            }
        })
    }

    drain () {
        return this._journalist.drain()
    }

    static async flush (writes) {
        for (const id in writes) {
            const queue = writes[id]
            if (!queue.written) {
                await queue.promise
            }
        }
    }
}

module.exports = Strata
