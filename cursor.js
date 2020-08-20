const assert = require('assert')

const Strata = { Error: require('./error') }
const find = require('./find')

class Cursor {
    constructor (journalist, descent, key) {
        this._entry = descent.entry
        this.page = this._entry.value
        this.found = descent.index >= 0
        this.sought = key
        this.index = descent.index < 0 ? ~descent.index : descent.index
        this.items = this.page.items
        this.ghosts = this.page.ghosts
        this._journalist = journalist
        this._promises = {}
    }

    // You must never use `indexOf` to scan backward for insert points, only to scan
    // backward for reading. Actually, let's assume that scanning will operate
    // directly on the `items` array and we're only going to use `indexOf` to search
    // forward for insertion points, and only forward.

    indexOf (key, index) {
        if (this.page.deleted) {
            return null
        }
        const comparator = this._journalist.comparator
        index = find(comparator, this.page, key, index)
        // TODO What about inserting at zero? Only if we are at the first page,
        // right?
        const unambiguous = -1 < index // <- TODO ?
            || ~ index < this.page.items.length
            || this.page.right == null
        if (!unambiguous && comparator(key, this.page.right) >= 0) {
            return null
        }
        return index
    }

    insert (value, key, index) {
        Strata.Error.assert(
            index > -1 &&
            (
                this.index > 0 ||
                this.page.id == '0.1'
            ), 'invalid.insert.index', { index: this.index })

        // Heft will be set when the record is serialized.
        assert(key && value)
        // Create a record to add to the page. Also give to Journalist so it can
        // set the heft.
        const record = { key: key, value: value, heft: 0 }

        this._journalist.append({
            id: this.page.id,
            record: record,
            header: { method: 'insert', index: index },
            parts: [
                this._journalist.serializer.key.serialize(key),
                this._journalist.serializer.value.serialize(value)
            ]
        }, this._promises)

        this.page.items.splice(index, 0, record)
    }

    remove (index) {
        const ghost = this.page.id != '0.1' && index == 0

        this._journalist.append({
            id: this.page.id,
            header: { method: 'delete', index: index },
            parts: []
        }, this._promises)

        if (ghost) {
            this.page.ghosts++
            this.ghosts++
        } else {
            const [ spliced ] = this.page.items.splice(index, 1)
            this._entry.heft -= spliced.heft
        }
    }

    async flush () {
        const promises = []
        for (let id in this._promises) {
            promises.push(this._promises[id])
        }
        this._promises = {}
        for (let promise of promises) {
            await promise
        }
    }

    release () {
        this._entry.release()
    }
}

module.exports = Cursor
