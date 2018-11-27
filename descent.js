var cadence = require('cadence')
var Locker = require('./locker')
var Sheaf = require('./sheaf')
var ok = require('assert').ok
var extend = require('./extend')

function Descent (sheaf, locker, override) {
    ok(sheaf instanceof Sheaf, 'sheaf')
    ok(locker instanceof Locker)

    override = override || {}

    this. exclusive = override.exclusive || false
    this.depth = override.depth == null ? -1 : override.depth
    this.indexes = override.indexes || {}
    this.sheaf = sheaf
    this.greater = override.greater
    this.lesser = override.lesser
    this.page = override.page
    this.index = override.index == null ? 0 : override.index
    this.locker = locker
    this.descent = {}

    if (!this.page) {
        this.locker.lock(-2, false, function (error, page) {
            ok(!error, 'impossible error')
            this.page = page
        }.bind(this))
        ok(this.page, 'dummy page not in cache')
    } else {
        this.locker.increment(this.page)
    }
}

Descent.prototype.setIndex = function (i) {
    this.indexes[this.page.address] = this.index = i
}

Descent.prototype.fork = function () {
    return new Descent(this.sheaf, this.locker, {
        page: this.page,
        exclusive: this.exclusive,
        depth: this.depth,
        index: this.index,
        indexes: extend({}, this.indexes)
    })
}

Descent.prototype.exclude = function () {
    this.exclusive = true
}

Descent.prototype.upgrade = cadence(function (async) {
    async([function () {
        this.locker.unlock(this.page)
        this.locker.lock(this.page.address, this.exclusive = true, async())
    }, function (error) {
        this.locker.lock(-2, false, function (error, locked) {
            ok(!error, 'impossible error')
            this.page = locked
        }.bind(this))
        ok(this.page, 'dummy page not in cache')
        throw error
    }], function (locked) {
        this.page = locked
    })
})

Descent.prototype.key = function (key) {
    return function () {
        return this.sheaf.find(this.page, key, this.page.address % 2 ? this.page.ghosts : 1)
    }
}

Descent.prototype.left = function () {
   return this.page.ghosts || 0
}

Descent.prototype.right = function () {
    return this.page.items.length - 1
}

Descent.prototype.found = function (keys) {
    return function () {
        return this.page.items[0].address != 0 && this.index != 0 && keys.some(function (key) {
            return this.sheaf.comparator(this.page.items[this.index].key,  key) == 0
        }, this)
    }
}

Descent.prototype.child = function (address) { return function () { return this.page.items[this.index].address == address } }

Descent.prototype.address = function (address) { return function () { return this.page.address == address } }

Descent.prototype.penultimate = function () { return this.page.items[0].address % 2 }

Descent.prototype.leaf = function () { return this.page.address % 2 }

Descent.prototype.level = function (level) {
    return function () { return level == this.depth }
}

Descent.prototype.unlocker = function (parent) {
    this.locker.unlock(parent)
}

Descent.prototype.descend = cadence(function (async, next, stop) {
    async.loop([], function () {
        if (stop.call(this)) {
            return [ async.break, this.page, this.index ]
        } else {
            if (this.index + 1 < this.page.items.length) {
                this.greater = this.page.address
            }
            if (this.index > 0) {
                this.lesser = this.page.address
            }
            this.locker.lock(this.page.items[this.index].address, this.exclusive, async())
        }
    }, function (locked) {
        this.depth++
        this.unlocker(this.page, locked)
        this.page = locked
        var index = next.call(this)
        if (!(this.page.address % 2) && index < 0) {
            this.setIndex((~index) - 1)
        } else {
            this.setIndex(index)
        }
        this.indexes[this.page.address] = this.index
        if (this.page.address % 2 === 0) {
            ok(this.page.items.length, 'page has addresses')
            ok(this.page.items[0].key == null, 'first key is cached')
        }
    })
})

module.exports = Descent
