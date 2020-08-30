const Journalist = require('./journalist')
const Cursor = require('./cursor')

class Unlocker {
    static Error = require('./error')

    constructor (cursor) {
        this._cursor = cursor
        cursor.page.lock = new Promise(resolve => this._lock = resolve)
    }

    get () {
        if (this._lock != null) {
            this._lock.call()
            this._lock = null
            this._cursor.page.lock = null
        }
        return this._cursor
    }
}

const NULL_CURSOR = {
    page: { ghosts: 0 },
    indexOf: function () { return null },
    release: function () {}
}

class Strata {
    static MIN = Symbol('MIN')

    static MAX = Symbol('MAX')

    constructor (destructible, options) {
        this._journalist = new Journalist(destructible, options)
    }

    create () {
        return this._journalist.create()
    }

    open () {
        return this._journalist.open()
    }

    static nullCursor () {
        return NULL_CURSOR
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
    async search (key, fork = false) {
        const query = key === Strata.MIN
            ? { key: null, rightward: false, fork: false }
            : key === Strata.MAX
                ? { key: null, rightward: true, fork: false }
                : { key, rightward: false, fork: fork, approximate: true }
        DESCEND: for (;;) {
            const descent = await this._journalist.descend(query)
            const cursor = new Cursor(this._journalist, descent, key)
            UNLOCK: while (cursor.page.lock != null) {
                descent.entry.release()
                await page.lock
                if ((cursor.index = cursor.indexOf(key, 0)) == null) {
                    cursor.release()
                    continue DESCEND
                }
                continue UNLOCK
            }
            return new Unlocker(cursor)
        }
    }

    static async flush (writes) {
        for (const id in writes) {
            const queue = writes[id]
            if (!queue.written) {
                await queue.promise
            }
        }
    }

    close () {
        return this._journalist.close()
    }
}

module.exports = Strata
