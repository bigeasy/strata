var cadence = require('cadence/redux')
var ok = require('assert').ok
var Queue = require('./queue')
var Scribe = require('./scribe')
var scram = require('./scram')

function Cursor (sheaf, descents, exclusive, searchKey) {
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
Cursor.prototype.indexOf = function (key, index) {
    ok(arguments.length == 2, 'index requires two arguments')
    var page = this._page
    var index = this._sheaf.find(page, key, index)
    var unambiguous
    unambiguous = -1 < index // <- todo: ?
               || ~ index < this._page.items.length
               || page.right.address == 0
    if (!unambiguous && this._sheaf.comparator(key, page.right.key) >= 0) {
        return [ ~(this._page.items.length + 1) ]
    }
    return index
}

Cursor.prototype._unlock = cadence(function (async) {
    async([function () {
        this._locker.unlock(this._page)
        this._locker.dispose()
    }], function () {
        this._appender.close(async())
    })
})

// todo: pass an integer as the first argument to force the arity of the
// return.
Cursor.prototype.unlock = function (callback) {
    if (this._appender) {
        this._unlock(callback)
    } else {
        this._locker.unlock(this._page)
        this._locker.dispose()
        callback()
    }
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

Cursor.prototype._filename = function (page) {
    return path.join(this._sheaf.directory, 'pages', pages.address + '.' + page.rotation)
}

Cursor.prototype.insert = function (record, key, index) {
    ok(this.exclusive, 'cursor is not exclusive')
    ok(index > 0 || this._page.address == 1)

    this._sheaf.unbalanced(this._page)

    if (this._appender == null) {
        this._appender = this._sheaf.logger.createAppender(this._page)
    }

    var length = this._appender.writeInsert(index, record)

    this._page.splice(index, 0, {
        key: key,
        record: record,
        heft: length
    })

    this.length = this._page.items.length
}

Cursor.prototype.remove = function (index) {
    var ghost = this._page.address != 1 && index == 0, entry
    this._sheaf.unbalanced(this._page)

    if (this._appender == null) {
        this._appender = this._sheaf.logger.createAppender(this._page)
    }

    this._appender.writeDelete(index)

    if (ghost) {
        this._page.ghosts++
        this.offset || this.offset++
    } else {
        this._page.splice(index, 1)
    }
}

module.exports = Cursor
