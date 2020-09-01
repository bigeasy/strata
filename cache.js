// Node.js API dependencies.
const assert = require('assert')

// An `Entry` is a refence-counted public reference to a value stored cache.

//
class Entry {
    // Construct an `Entry` that is a member of the given `cache`, stored in the
    // cache's map with the given `stringified` key and that caches the given
    // `value`.
    constructor (cache, stringified, value) {
        this._cache = cache
        this._stringified = stringified
        this.when = Date.now()
        this._next = this._cache._head._next
        this._previous =  this._cache._head
        this._next._previous = this
        this._previous._next = this
        this.value = value
        this._references = 1
        this._heft = 0
    }

    // Release the `Entry` decreasting it's reference count by one. When the
    // reference count reaches zero the entry is a candidate for removal from
    // the cache during a [`Cache.purge`](#purge)
    release () {
        this._references--
    }

    // Remove the `Entry` from the cache. You must hold the only reference to
    // the entry in order to remove it.
    remove () {
        assert.equal(this._references, 1)
        this._cache._remove(this)
    }

    // <a name="heft">Assign</a> an arbitrary measure of the weight of the
    // entry. Since we have no way of knowing the amount of space a JavaScript
    // object occupies in RAM, we use the size of the record as it is serialized
    // to disk to represent the entry size and we call it our entry's "heft" so
    // we're reminded that it is a relative size not an accurate one.
    //
    // Setting the heft of an entry adjusts the heft of the associated cache. We
    // use [`Cache.purge`](#purge) to attempt to reduce the total heft of the
    // cache to a desired heft.
    set heft (heft) {
        this._cache.heft -= this._heft
        this._cache.heft += (this._heft = heft)
    }

    // Get the arbitrary weight of the entry. See [`Entry.heft`](#heft).
    get heft () {
        return this._heft
    }
}

class Cache {
    // Construct a new empty cache.
    constructor () {
        this._map = {}
        this._head = { _cache: null, _next: null, _previous: null }
        this._head._next = this._head._previous = this._head
        this.entries = 0
        this.heft = 0
    }

    // Hold a reference to an entry in cache indexed by the given `key` which is
    // an `Array` that of JSON objects that uniquely identify the entry in the
    // cache. If the entry does not exist one is created using the value given
    // by `initializer`.
    hold (key, initializer) {
        const stringified = JSON.stringify(key)
        const entry = this._map[stringified]
        if (entry == null) {
            this.entries++
            return this._map[stringified] = new Entry(this, stringified, initializer)
        }
        entry._next._previous = entry._previous
        entry._previous._next = entry._next

        entry._next = this._head._next
        entry._previous = this._head
        entry._next._previous = entry
        entry._previous._next = entry

        entry._references++
        return entry
    }

    _remove (entry) {
        this.heft -= entry._heft
        entry._cache = null
        entry._next._previous = entry._previous
        entry._previous._next = entry._next
        delete this._map[entry._stringified]
        this.entries--
    }

    purge (heft) {
        let iterator = this._head._previous
        while (this.heft > heft && iterator._cache != null) {
            if (iterator._references == 0) {
                // TODO Get rid of this assertion after a while. Also remove
                // heft update in `Journalist` it has a TODO.
                if (iterator.value.leaf) {
                    const page = iterator.value
                    assert.equal(iterator.heft, page.items.slice(0).reduce((sum, item) => {
                        return sum + item.heft
                    }, 1))
                }
                this._remove(iterator)
            }
            iterator = iterator._previous
        }
    }
}

module.exports = Cache
