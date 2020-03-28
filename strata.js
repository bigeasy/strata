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

class Strata {
    constructor (destructible, options) {
        this._journalist = new Journalist(destructible, options)
    }

    create () {
        return this._journalist.create()
    }

    open () {
        return this._journalist.open()
    }

    async search (key) {
        DESCEND: for (;;) {
            const descent = await this._journalist.descend({ key })
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

    close () {
        return this._journalist.close()
    }
}

module.exports = Strata
