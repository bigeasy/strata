const assert = require('assert')

const Strata = { Error: require('./error') }
const find = require('./find')

class Cursor {
    constructor (journalist, descent, key) {
        this.page = descent.entry.value
        this.sought = key
        this._entry = descent.entry
        this._journalist = journalist
        this._promises = {}
    }

    get found () {
        throw new Error('removed')
    }

    get index () {
        throw new Error('removed')
    }

    get items () {
        throw new Error('removed')
    }

    get ghosts () {
        throw new Error('removed')
    }

    // You must never use `indexOf` to scan backward for insert points, only to scan
    // backward for reading. Actually, let's assume that scanning will operate
    // directly on the `items` array and we're only going to use `indexOf` to search
    // forward for insertion points, and only forward.

    //
    indexOf (key, offset = 0) {
        if (this.page.deleted) {
            return null
        }
        const comparator = this._journalist.comparator
        let index = find(comparator, this.page, key, Math.max(offset, this.page.ghosts))
        // Unambiguous if we actually found it.
        if (-1 < index) {
            return { index: index, found: true }
        }
        // We only insert before the key on the left most page.
        if (~index == 0) {
            return this.page.id == '0.1'
                ? { index: ~index, found: false }
                : { index: null, found: false }
        }
        // No problem if the index is within the exiting set of items, or if
        // this is the right most page.
        if (~index < this.page.items.length || this.page.right == null) {
            return { index: ~index, found: false }
        }
        // Otherwise we should ensure that the key is less than the key of the
        // right to the right.
        return comparator(key, this.page.right) < 0
            ? { index: ~index, found: false }
            : { index: null, found: false }
    }

    // Insert a record into the b-tree. Parts is an array of objects in their
    // deserialized form that will be serialized using the parts serializer. The
    // key for the record will be obtained using the key extractor.
    //
    // The index should be either the index returned from the initial descent of
    // the tree for the value used to desecend the tree or for subsequent
    // inserts using `Cursor.indexOf` if `Cursor.indexOf` does not return
    // `null`.

    //
    insert (index, key, parts, writes = {}) {
        Strata.Error.assert(
            index > -1 &&
            (
                index > 0 ||
                this.page.id == '0.1'
            ), 'invalid.insert.index', { index: index })

        const record = { key: key, parts: parts, heft: 0 }

        // Create a record to add to the page. Also give to Journalist so it can
        // set the heft.
        this._journalist.append({
            id: this.page.id,
            header: { method: 'insert', index: index },
            parts: this._journalist.serializer.parts.serialize(parts),
            record: record
        }, writes)

        this.page.items.splice(index, 0, record)
    }

    remove (index, writes = {}) {
        const ghost = this.page.id != '0.1' && index == 0

        this._journalist.append({
            id: this.page.id,
            header: { method: 'delete', index: index },
            parts: []
        }, writes)

        if (ghost) {
            this.page.ghosts++
            this._entry.heft -= this.page.items[0].heft
        } else {
            const [ spliced ] = this.page.items.splice(index, 1)
            this._entry.heft -= spliced.heft
        }
    }

    release () {
        this._entry.release()
    }
}

module.exports = Cursor
