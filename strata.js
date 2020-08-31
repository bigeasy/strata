const Journalist = require('./journalist')
const Cursor = require('./cursor')

const NULL_CURSOR = {
    page: { ghosts: 0, items: [], id: null },
    indexOf: function () { return null },
    release: function () {}
}

class Strata {
    static MIN = Symbol('MIN')

    static MAX = Symbol('MAX')

    constructor (destructible, options) {
        this._journalist = new Journalist(destructible, options)
        const { comparator, extractor }  = this._journalist
        this.compare = function (left, right) { return comparator(left, right) }
        this.extract = function (parts) { return extractor(parts) }
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
        const descent = await this._journalist.descend(query)
        return new Cursor(this._journalist, descent, key)
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
