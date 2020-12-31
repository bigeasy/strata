const fs = require('fs').promises

const Sheaf = require('./sheaf')
const Cursor = require('./cursor')
const assert = require('assert')

const Magazine = require('magazine')

class Strata {
    static MIN = Symbol('MIN')

    static MAX = Symbol('MAX')

    static Error = require('./error')

    constructor (destructible, options) {
        this.destructible = destructible
        this._sheaf = new Sheaf(destructible, options)
        const { comparator, extractor }  = this._sheaf
        this.compare = function (left, right) { return comparator.leaf(left, right) }
        console.log('called', this.compare)
        this.extract = function (parts) { return extractor(parts) }
    }

    static open (destructible, options) {
        const strata = new Strata(destructible, options)
        return options.create ? strata._sheaf.create(strata) : strata._sheaf.open(strata)
    }

    get pages () {
        return this._sheaf.pages
    }

    get handles () {
        return this._sheaf.handles
    }

    get extractor () {
        return this._sheaf.extractor
    }

    get serializer () {
        return this._sheaf.serializer
    }

    get checksum () {
        return this._sheaf.checksum
    }

    // What was the lock for? It was to ensure that another strand doesn't
    // change the location of the index between in time it takes return from the
    // async call to `Strata.search`.
    //
    // TODO A race condition occurred to you. What if the page is deleted in
    // during some window and the cursor is invalid, but our descent is itself
    // synchornous, except now we can see below that it isn't, the call to
    // `Sheaf.descend` introduces the problem we tried to resolve with our lock,
    // so we ought to move the lock into `Sheaf`.

    //
    search (trampoline, key, ...vargs) {
        const [ fork, found ] = vargs.length == 2 ? vargs : [ false, vargs[0] ]
        const query = key === Strata.MIN
            ? { key: null, rightward: false, fork: false }
            : key === Strata.MAX
                ? { key: null, rightward: true, fork: false }
                : { key, rightward: false, fork: fork, approximate: true }
        this._sheaf.descend2(trampoline, query, descent => {
            const cursor = new Cursor(this._sheaf, descent, key)
            try {
                found(cursor)
            } finally {
                cursor.release()
            }
        })
    }

    drain () {
        return this._sheaf.drain()
    }

    static async flush (writes) {
        for (const id in writes) {
            const latch = writes[id]
            if (!latch.unlocked) {
                await latch.promise
            }
        }
    }
}

module.exports = Strata
