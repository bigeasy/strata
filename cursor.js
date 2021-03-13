const assert = require('assert')

const Fracture = require('fracture')

const find = require('./find')

class Cursor {
    constructor (sheaf, descent, key) {
        this.page = descent.entry.value
        this.sought = key
        this.index = descent.index
        this.found = descent.found
        this._entry = descent.entry
        this._sheaf = sheaf
        this._promises = {}
    }

    // You must never use `indexOf` to scan backward for insert points, only to
    // scan backward for reading. Actually, let's assume that scanning will
    // operate directly on the `items` array and we're only going to use
    // `indexOf` to search forward for insertion points, and only forward.

    //
    indexOf (key, offset = 0) {
        if (this.page.deleted) {
            return { index: null, found: false }
        }
        const comparator = this._sheaf.comparator.leaf
        let index = find(comparator, this.page.items, key, offset)
        // Unambiguous if we actually found it.
        if (-1 < index) {
            return { index: index, found: true }
        }
        // We only insert before the key on the left most page.
        if (~index == 0) {
            return this.page.id == '0.1' || comparator(this.page.key, key) <= 0
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

    // A way to get a heft prior to insert, but we won't know the index. We
    // could get a partial heft, though, just the heft of the parts. Does this
    // do any good, though? In Memento we add a part ourselves, and it changes
    // when we insert into the tree. Maybe we somehow pre-serialize then insert
    // into the tree a buffer and the tree doesn't do serialization? But in
    // Strata we need to serialize when we balance, so we can't have
    // deserialization that doesn't match serialization, not unless we specify
    // separate serializers for insert and rewrite. Oh, and `insert` does need
    // both the serialized and deserialized parts anyway.

    //
    serialize (parts) {
        return this._sheaf.storage.serializer.parts.serialize(parts)
    }

    /* Maybe this instead.
    record (key, parts) {
        return { key: key, parts: parts, buffers = this.serialize(parts) }
    }
    */

    // Insert a record into the b-tree. Parts is an array of objects in their
    // deserialized form that will be serialized using the parts serializer. The
    // key for the record will be obtained using the key extractor.
    //
    // The index should be either the index returned from the initial descent of
    // the tree for the value used to desecend the tree or for subsequent
    // inserts using `Cursor.indexOf` if `Cursor.indexOf` does not return
    // `null`.

    //
    insert (stack, index, key, parts, buffers = this.serialize(parts)) {
        assert(stack instanceof Fracture.Stack)
        const header = { method: 'insert', index: index }
        const buffer = this._sheaf.storage.recordify(header, buffers)
        const record = { key: key, parts: parts, heft: buffer.length }

        this._entry.heft += record.heft

        this.page.items.splice(index, 0, record)

        return this._sheaf.append(stack, this.page.id, buffer)
    }

    remove (stack, index) {
        assert(stack instanceof Fracture.Stack)
        const header = { method: 'delete', index: index }
        const buffer = this._sheaf.storage.recordify(header, [])

        const [ spliced ] = this.page.items.splice(index, 1)
        this._entry.heft -= spliced.heft

        this.page.deletes++

        return this._sheaf.append(stack, this.page.id, buffer)
    }

    // **TODO** The user no longer releases the cursor.
    release () {
        this._entry.release()
    }
}

module.exports = Cursor
