const assert = require('assert')

const Strata = { Error: require('./error') }
const find = require('./find')

class Cursor {
    constructor (journalist, descent, key) {
        this._entry = descent.entries.pop()
        this._page = this._entry.value
        descent.entries.forEach(entry => entry.release())
        this.found = descent.index >= 0
        this.sought = key
        this.index = descent.index < 0 ? ~descent.index : descent.index
        this.items = this._page.items
        this.ghosts = this._page.ghosts
        this._journalist = journalist
        this._promises = {}
    }

    // You must never use `indexOf` to scan backward for insert points, only to scan
    // backward for reading. Actually, let's assume that scanning will operate
    // directly on the `items` array and we're only going to use `indexOf` to search
    // forward for insertion points, and only forward.

    indexOf (key, index) {
        const comparator = this._journalist.comparator
        index = find(comparator, this._page, key, index)
        const unambiguous = -1 < index // <- TODO ?
            || ~ index < this._page.items.length
            || this._page.right == null
        if (!unambiguous && comparator(key, page.right) >= 0) {
            return null
        }
        return index
    }

    insert (value, key, index) {
        Strata.Error.assert(
            index > -1 &&
            (
                this.index > 0 ||
                this._page.id == '0.1'
            ), 'invalid.insert.index', { index: this.index })

        // Heft will be set when the record is serialized.
        assert(key && value)
        const record = { key: key, value: value, heft: 0 }

        this._journalist.append({
            id: this._page.id,
            record: record,
            header: { method: 'insert', index: index, key: key },
            body: value
        }, this._promises)

        this._page.items.splice(index, 0, record)
    }

    remove (index) {
        const ghost = this._page.id != '0.1' && index == 0

        this._journalist.append({
            id: this._page.id,
            header: { method: 'delete', index: index },
            body: null
        }, this._promises)

        if (ghost) {
            this._page.ghosts++
            this.ghosts++
        } else {
            const [ spliced ] = this._page.items.splice(index, 1)
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
