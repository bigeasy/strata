var cadence = require('cadence/redux')
var ok = require('assert').ok
var Queue = require('./queue')
var scram = require('./scram')

function Cursor (sheaf, journal, descents, exclusive, searchKey) {
    this._journal = journal
    this._sheaf = sheaf
    this._locker = descents[0].locker
    this._page = descents[0].page
    this._searchKey = searchKey
    this.exclusive = exclusive
    this.index = descents[0].index
    this.offset = this.index < 0 ? ~ this.index : this.index

    descents.shift()
}

Cursor.prototype.get = function (index) {
    return this._page.items[index]
}

// to user land
Cursor.prototype.next = cadence(function (async) {
    var next

    if (this._page.right.address == 0) {
        // return [ async, false ] <- return immediately!
        return [ false ]
    }

    async(function () {
        this._locker.lock(this._page.right.address, this.exclusive, async())
    }, function (next) {
        this._locker.unlock(this._page)

        this._page = next

        this.offset = this._page.ghosts
        this.length = this._page.items.length

        return [ true ]
    })
})

// to user land
Cursor.prototype._indexOf = function (key) {
    var page = this._page
    var index = this._sheaf.find(page, key, page.ghosts)
    var unambiguous
    unambiguous = -1 < index // <- todo: ?
               || ~ index < this._page.items.length
               || page.right.address == 0
    if (!unambiguous && this._sheaf.comparator(key, page.right.key) >= 0) {
        return [ ~(this._page.items.length + 1) ]
    }
    return index
}

// todo: pass an integer as the first argument to force the arity of the
// return.
Cursor.prototype._unlock = cadence(function (async) {
    async([function () {
        this._locker.unlock(this._page)
        this._locker.dispose()
    }], function () {
        if (this.queue) {
            this.queue.finish()
            if (this.queue.buffers.length) {
                this._write(async())
            }
        }
    }, function () {
        this._journal.close('leaf', async())
    }, function () {
        return []
    })
})

Cursor.prototype.unlock = function (callback) {
    ok(callback, 'unlock now requires a callback')
    this._unlock(callback)
}

// note: exclusive, index, offset and length are public

Cursor.prototype.__defineGetter__('address', function () {
    return this._page.address
})

Cursor.prototype.__defineGetter__('right', function () {
    return this._page.right.address
})

Cursor.prototype.__defineGetter__('right_', function () {
    return this._page.right
})

Cursor.prototype.__defineGetter__('ghosts', function () {
    return this._page.ghosts
})

Cursor.prototype.__defineGetter__('length', function () {
    return this._page.items.length
})

Cursor.prototype._write = cadence(function (async) {
    var entry
    async(function () {
        entry = this._journal.open(this._sheaf.filename2(this._page), this._page.position, this._page)
        this._sheaf.journalist.purge(async())
    }, function () {
        entry.ready(async())
    }, function () {
        this._page.position += this.queue.length
        scram.call(this, entry, cadence(function (async) {
            async(function () {
                async.forEach(function (buffer) {
                    entry.write(buffer, async())
                })(this.queue.buffers)
            }, function () {
                this.queue.clear()
                async(function () {
                    entry.close('entry', async())
                }, function () {
                    return []
                })
            })
        }), async())
    })
})

Cursor.prototype.insert = cadence(function (async, record, key, index) {
    ok(this.exclusive, 'cursor is not exclusive')
    ok(index > 0 || this._page.address == 1)

    this._sheaf.unbalanced(this._page)

    if (!this.queue) {
        this.queue = new Queue
    }

    var length = this._sheaf.writeInsert(this.queue, this._page, index, record)
    this._sheaf.splice(this._page, index, 0, {
        key: key,
        record: record,
        heft: length
    })
    this.length = this._page.items.length

    if (this.queue.buffers.length) {
        this._write(async())
    }
})

Cursor.prototype.remove = cadence(function (async, index) {
    var ghost = this._page.address != 1 && index == 0, entry
    this._sheaf.unbalanced(this._page)

    if (!this.queue) {
        this.queue = new Queue
    }

    this._sheaf.writeDelete(this.queue, this._page, index)
    if (ghost) {
        this._page.ghosts++
        this.offset || this.offset++
    } else {
        this._sheaf.splice(this._page, index, 1)
    }

    if (this.queue.buffers.length) {
        this._write(async())
    }
})

module.exports = Cursor
