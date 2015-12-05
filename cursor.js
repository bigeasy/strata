var cadence = require('cadence')
var ok = require('assert').ok
var Queue = require('./queue')
var Scribe = require('./scribe')
var scram = require('./scram')

function Cursor (sheaf, logger, descents, exclusive, searchKey) {
    this.sheaf = sheaf
    this.logger = logger
    this._locker = descents[0].locker
    this.page = descents[0].page
    this.searchKey = searchKey
    this.exclusive = exclusive
    this.index = descents[0].index
    this.offset = this.index < 0 ? ~this.index : this.index
    descents.shift()
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

Cursor.prototype.indexOf = function (key, index) {
    ok(arguments.length == 2, 'index requires two arguments')
    var page = this.page
    var index = this.sheaf.find(page, key, index)
    var unambiguous
    unambiguous = -1 < index // <- TODO ?
               || ~ index < this.page.items.length
               || page.right.address === null
    if (!unambiguous && this.sheaf.comparator(key, page.right.key) >= 0) {
        return [ ~(this.page.items.length + 1) ]
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
    ok(this.exclusive, 'cursor is not exclusive')
    ok(index > 0 || this.page.address == 1)

    this.sheaf.unbalanced(this.page)

    if (this._appender == null) {
        this._appender = this.logger.createAppender(this.page)
    }

    var heft = this._appender.writeInsert(index, record).length
    this.page.splice(index, 0, {
        key: key,
        record: record,
        heft: heft
    })

    this.length = this.page.items.length
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

module.exports = Cursor
