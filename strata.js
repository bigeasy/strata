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
        this._sheaf = new Sheaf(destructible, options)
    }

    static open (destructible, options) {
        const strata = new Strata(destructible, options)
        return options.create ? strata._sheaf.create(strata) : strata._sheaf.open(strata)
    }

    get options () {
        return this._sheaf.options
    }

    get destructible () {
        return this._sheaf.destructible
    }

    get deferrable () {
        return this._sheaf.deferrable
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
