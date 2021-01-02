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

    // **TODO** Just return `sheaf.options`.
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
            const future = writes[id]
            if (!future.fulfilled) {
                await future.promise
            }
        }
    }
}

module.exports = Strata
