var cadence = require('cadence/redux')
var prototype = require('pointcut').prototype
var ok = require('assert').ok

function Locker (sheaf, magazine) {
    ok(arguments.length, 'no arguments')
    ok(magazine)
    this._locks = {}
    this._sheaf = sheaf
    this._magazine = magazine
}

prototype(Locker, 'lock', cadence(function (async, address, exclusive) {
    var cartridge = this._magazine.hold(address, {}),
        page = cartridge.value.page,
        locked

    ok(address != null, 'x' + address)
    ok(!this._locks[address], 'address already locked by this locker')

    if (!page)  {
        if (address % 2) {
            page = this._sheaf.createLeaf({ address: address })
        } else {
            page = this._sheaf.createBranch({ address: address })
        }
        cartridge.value.page = page
        var loaded = function (error) {
            if (error) {
                cartridge.value.page = null
                cartridge.adjustHeft(-cartridge.heft)
            }
            this._locks[page.address].unlock(error, page)
        }.bind(this)
        this._locks[page.address] = page.queue.createLock()
        this._locks[page.address].exclude(function () {
            if (page.address % 2) {
                this._sheaf.readLeaf(page, loaded)
            } else {
                this._sheaf.readBranch(page, loaded)
            }
        }.bind(this))
    } else {
        this._locks[page.address] = page.queue.createLock()
    }

    async([function () {
        async(function () {
            this._locks[page.address][exclusive ? 'exclude' : 'share'](async())
        },
        function () {
            this._sheaf.tracer('lock', { address: address, exclusive: exclusive }, async())
        }, function () {
            locked = true
            return [ page ]
        })
    }, function (error) {
        // todo: if you don't return something, then the return is the
        // error, but what else could it be? Document that behavior, or
        // set a reasonable default.
        this._magazine.get(page.address).release()
        this._locks[page.address].unlock(error)
        delete this._locks[page.address]
        throw error
    }])
}))

Locker.prototype.encache = function (page) {
    this._magazine.hold(page.address, { page: page })
    this._locks[page.address] = page.queue.createLock()
    this._locks[page.address].exclude(function () {})
    return page
}

Locker.prototype.checkCacheSize = function (page) {
    var heft = page.items.reduce(function (heft, item) { return heft + item.heft }, 0)
    ok(heft == this._magazine.get(page.address).heft, 'sizes are wrong')
}

Locker.prototype.unlock = function (page) {
    this.checkCacheSize(page)
    this._locks[page.address].unlock(null, page)
    if (!this._locks[page.address].count) {
        delete this._locks[page.address]
    }
    this._magazine.get(page.address).release()
}

Locker.prototype.increment = function (page) {
    this._locks[page.address].increment()
    this._magazine.hold(page.address)
}

Locker.prototype.dispose = function () {
    ok(!Object.keys(this._locks).length, 'locks outstanding')
    this._locks = null
}

module.exports = Locker
