var cadence = require('cadence')
var ok = require('assert').ok
var Queue = require('./queue')
var Scribe = require('./scribe')

var find = require('./find')

var Interrupt = require('interrupt').createInterrupter('b-tree')

function Cursor (journalist, cartridge, key, index) {
    ok(journalist)
    this._cartridge = cartridge
    this.found = index >= 0
    this.sought = key
    this.index = index < 0 ? ~index : index
    this.items = cartridge.value.items
    this.ghosts = cartridge.value.ghosts
    this._journalist = journalist
}

Cursor.prototype.next = cadence(function (async) {
    var next

    if (this.page.right.address === null) {
        // return [ async, false ] <- return immediately!
        return [ false ]
    }

    async(function () {
        this._locker.lock(this.page.right.address, this.exclusive, async())
    }, function (next) {
        this._locker.unlock(this.page)

        this.page = next

        this.offset = this.page.ghosts
        this.length = this.page.items.length

        return [ true ]
    })
})

// Restore your cursor when you return from asynchronous operations.

//
Cursor.prototype.seek = function (key, index) {
    var items = this._cartridge.value.items
    if (items.length == 0) {
        return -1
    }
    var compare = this._sheaf.compare(key, items[index].key)
    if (compare == 0) {
        return index
    }
    if (compare < 0) {
        for (;;) {
            index++
            if (index == items.length) {
                return -1
            }
            compare = this._sheaf.compare(key, items[index].key)
            if (compare == 0) {
                return index
            } else if (compare > 0) {
                return -1
            }
        }
    }
    for (;;) {
        index--
        if (index == -1) {
            return -1
        }
        compare = this._sheaf.compare(key, items[index].key)
        if (compare == 0) {
            return index
        } else if (compare < 0) {
            return -1
        }
    }
}

// You must never use `indexOf` to scan backward for insert points, only to scan
// backward for reading. Actually, let's assume that scanning will operate
// directly on the `items` array and we're only going to use `indexOf` to search
// forward for insertion points, and only forward.

Cursor.prototype.indexOf = function (key, index) {
    ok(arguments.length == 2, 'index requires two arguments')
    var page = this._cartridge.value
    var index = find(this._journalist.comparator, page, key, index)
    var unambiguous
    unambiguous = -1 < index // <- TODO ?
               || ~ index < page.items.length
               || page.right == null
    if (!unambiguous && this.sheaf.comparator(key, page.right.key) >= 0) {
        return null
    }
    return index
}

Cursor.prototype._unlock = cadence(function (async) {
    async([function () {
        this._locker.unlock(this.page)
        this._locker.dispose()
    }], function () {
        this._appender.close(async())
    })
})

// TODO pass an integer as the first argument to force the arity of the
// return.
Cursor.prototype.unlock = function (callback) {
    if (this._appender) {
        this._unlock(callback)
    } else {
        this._locker.unlock(this.page)
        this._locker.dispose()
        callback()
    }
}

Cursor.prototype.insert = function (record, key, index) {
    Interrupt.assert(this.index > 0 || this._cartridge.value.id == 1, 'invalid.insert.index', { index: this.index })
    // Forgot where I was with splitting, if I queue for a split check at the
    // moment I see the count increase, or, well, when else would you do it?
    // Probably do it now, because now is the time to split, but maybe that
    // split is canceled when the time comes.

    // TODO Okay, how do I write this out?
    var serialized = Buffer.from(JSON.stringify(record) + '\n')
    var heft = serialized.length

    // Okay, now we have a buffer and heft.
    var signal = this._journalist.append({
        id: this._cartridge.value.id,
        method: 'insert',
        index: index,
        serialized: serialized
    })

    // TODO Restore heft, this is a temporary heft. We're going to want to
    // calculate heft by calculating our serialization at this point. We may use
    // a common serializer, one we imagined we'd create in Conduit or Procession.
    this._cartridge.value.items.splice(index, 0, {
        key: key,
        record: record,
        heft: heft
    })
    this._cartridge.adjustHeft(heft)

    return signal
}

Cursor.prototype.remove = function (index) {
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

Cursor.prototype.close = function () {
    this._cartridge.release()
}

module.exports = Cursor
