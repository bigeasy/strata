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

    get storage () {
        return this._sheaf.storage
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

    descend (trampoline, comparator, key, found) {
        this._search(trampoline, {
            key: key,
            rightward: false,
            fork: false,
            comparator: { branch: comparator, leaf: comparator }
        }, found)
    }

    search (trampoline, key, ...vargs) {
        const [ fork, found ] = vargs.length == 2 ? vargs : [ false, vargs[0] ]
        const query = key === Strata.MIN
            ? { key: null, rightward: false, fork: false }
            : key === Strata.MAX
                ? { key: null, rightward: true, fork: false }
                : { key, rightward: false, fork: fork, approximate: true }
        this._sheaf.search(trampoline, query, descent => {
            const cursor = new Cursor(this._sheaf, descent, key)
            try {
                found(cursor)
            } finally {
                cursor.release()
            }
        })
    }

    _search (trampoline, query, found) {
        this._sheaf.search(trampoline, query, descent => {
            const cursor = new Cursor(this._sheaf, descent, query.key)
            try {
                found(cursor)
            } finally {
                cursor.release()
            }
        })
    }

    min (trampoline, found) {
        this._search(trampoline, { key: null, rightward: false, fork: false }, found)
    }

    max (trampoline, found) {
        this._search(trampoline, { key: null, rightward: true, fork: false }, found)
    }

    find (trampoline, key) {
        this._search(trampoline, { key, rightward: false, fork: false }, found)
    }

    fork (trampoline, found) {
        this._search(trampoline, { key: null, rightward: false, fork: true }, found)
    }

    drain () {
        return this._sheaf.drain()
    }
}

module.exports = Strata
