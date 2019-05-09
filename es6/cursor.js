var assert = require('assert')

var find = require('./find')

var Interrupt = require('interrupt').createInterrupter('b-tree')

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
        this._signals = {}
    }

    // You must never use `indexOf` to scan backward for insert points, only to scan
    // backward for reading. Actually, let's assume that scanning will operate
    // directly on the `items` array and we're only going to use `indexOf` to search
    // forward for insertion points, and only forward.

    indexOf (key, index) {
        const comparator = this._journalist.comparator
        const page = this._cartridge.value
        index = find(comparator, page, key, index)
        const unambiguous = -1 < index // <- TODO ?
            || ~ index < this._page.items.length
            || this._page.right == null
        if (!unambiguous && comparator(key, page.right) >= 0) {
            return null
        }
        return index
    }

    release () {
        this._entry.release()
    }

    insert (value, key, index) {
        Interrupt.assert(index > -1 && (this.index > 0 || this._cartridge.value.id == '0.1'), 'invalid.insert.index', { index: this.index })
        // Forgot where I was with splitting, if I queue for a split check at the
        // moment I see the count increase, or, well, when else would you do it?
        // Probably do it now, because now is the time to split, but maybe that
        // split is canceled when the time comes.

        // TODO Okay, how do I write this out?
        // TODO You need to write the operation as a header and then write the body,
        // that way you can have the heft right there.
        var body = Buffer.from(JSON.stringify({ key: key, value: value }))
        var heft = body.length

        // Okay, now we have a buffer and heft.
        this._journalist.append({
            id: this._cartridge.value.id,
            header: { method: 'insert', index: index, json: true },
            body: body
        }, this._signals)

        // TODO Restore heft, this is a temporary heft. We're going to want to
        // calculate heft by calculating our serialization at this point. We may use
        // a common serializer, one we imagined we'd create in Conduit or Procession.
        this._cartridge.value.items.splice(index, 0, { key: key, value: value, heft: heft })
        this._cartridge.adjustHeft(heft)
    }

    async flush () {
        var signals = []
        for (var id in this._signals) {
            signals.push(this._signals[id])
        }
        async.forEach([ signals ], function (signal) {
            signal.wait(async())
        })
    }

    remove (index) {
        var ghost = this.page.address != 1 && index == 0, entry
        this.sheaf.unbalanced(this.page)

        if (this._appender == null) {
            this._appender = this.logger.createAppender(this.page)
        }

        this._appender.writeDelete(index)

        if (ghost) {
            this.page.ghosts++
            this.offset || this.offset++
        } else {
            this.page.splice(index, 1)
        }
    }
}

module.exports = Cursor
