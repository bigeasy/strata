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

    search (trampoline, ...vargs) {
        const key = vargs.shift()
        const found = vargs.pop()
        let partial = Number.MAX_SAFE_INTEGER, fork = false
        while (vargs.length != 0) {
            switch (typeof vargs[0]) {
            case 'boolean':
                fork = vargs.shift()
                break
            case 'number':
                partial = vargs.shift()
                break
            }
        }
        const query = {
            key: key === Strata.MIN ? null : key,
            rightward: key === Strata.MAX,
            fork: fork,
            partial: partial,
            approximate: true
        }
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

    find (trampoline, key, found) {
        this._search(trampoline, { key, rightward: false, fork: false, approximate: true }, found)
    }

    fork (trampoline, key, found) {
        this._search(trampoline, { key, rightward: false, fork: true, approximate: true }, found)
    }

    after (trampoline, key, partial, found) {
        key = key.concat(null)
        this._search(trampoline, { key, partial: key.length - 1, rightward: false, fork: false, approximate: true }, found)
    }

    drain () {
        return this._sheaf.drain()
    }
}

module.exports = Strata
