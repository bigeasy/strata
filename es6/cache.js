const assert = require('assert')

class Entry {
    constructor (cache, stringified, value) {
        this._cache = cache
        this._stringified = stringified
        this.when = Date.now()
        this.next = this._cache._head.next
        this.previous =  this._cache._head
        this.next.previous = this
        this.previous.next = this
        this.value = value
        this._references = 1
        this._heft = 0
    }

    release () {
        this._references--
    }

    remove () {
        assert.equal(this._references, 1)
        this._cache._remove(this)
    }

    set heft (heft) {
        this._cache.heft -= this._heft
        this._cache.heft += (this._heft = heft)
    }

    get heft () {
        return this._heft
    }
}

class Cache {
    constructor () {
        this._map = {}
        this._head = { _cache: null, next: null, previous: null }
        this._head.next = this._head.previous = this._head
        this.heft = 0
    }

    hold (key, initializer) {
        const stringified = JSON.stringify(key)
        const entry = this._map[stringified]
        if (entry == null) {
            return this._map[stringified] = new Entry(this, stringified, initializer)
        }
        entry.next.previous = entry.previous
        entry.previous.next = entry.next

        entry.next = this._head.next
        entry.previous = this._head
        entry.next.previous = entry
        entry.previous.next = entry

        entry._references++
        return entry
    }

    _remove (entry) {
        this.heft -= entry._heft
        entry._cache = null
        entry.next.previous = entry.previous
        entry.previous.next = entry.next
        delete this._map[entry._stringified]
    }

    purge (heft) {
        let iterator = this._head.previous
        while (this.heft > heft && iterator._cache != null) {
            if (iterator._references == 0) {
                this._remove(iterator)
            }
            iterator = iterator.previous
        }
    }
}

module.exports = Cache
