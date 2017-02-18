var cadence = require('cadence')
var ok = require('assert').ok

function Locker (sheaf, magazine) {
    ok(arguments.length, 'no arguments')
    ok(magazine)
    this._locks = {}
    this._sheaf = sheaf
    this._magazine = magazine
}

Locker.prototype.lock = cadence(function (async, address, exclusive) {
    // TODO locks does not appear to need to be an array.
    var cartridge = this._magazine.hold(address, {}),
        page = cartridge.value.page, locks = []

    ok(address != null, 'x' + address)
    ok(!this._locks[address], 'address already locked by this locker')

    async([function () {
        if (page == null) {
            // Note: This catch block is only good for catching read errors.
            // It untestable to catch errors in the first step that locks
            // the page, which is all tested, synchronous code. Instead of
            // throwing the error, we give it to the lock. The primary lock
            // sub-cadence will receive the error and raise it to be caught
            // by the final external catch block.
            async([function () {
                async(function () {
                    // TODO does there need to be differnt types anymore?
                    page = this._sheaf.createPage(address % 2, address)
                    cartridge.value.page = page
                    page.cartridge = cartridge
                    locks.push(this._locks[address] = page.queue.createLock())
                    locks[0].exclude(async())
                }, function () {
                    this._sheaf.player.read(this._sheaf, page, async())
                }, function () {
                    locks[0].unlock()
                })
            }, function (error) {
                cartridge.value.page = null
                cartridge.adjustHeft(-cartridge.heft)
                locks[0].unlock(error)
            }])
        } else {
            locks.push(this._locks[address] = page.queue.createLock())
        }
        async(function () {
            this._locks[page.address][exclusive ? 'exclude' : 'share'](async())
        }, function () {
            this._sheaf.tracer('lock', { address: address, exclusive: exclusive }, async())
        }, function () {
            return [ page ]
        })
    }, function (error) {
        cartridge.release()
        delete this._locks[page.address]
        locks.forEach(function (lock) {
            lock.unlock(error)
        })
        throw error
    }])
})

Locker.prototype.encache = function (page) {
    page.cartridge = this._magazine.hold(page.address, { page: page })
    this._locks[page.address] = page.queue.createLock()
    this._locks[page.address].exclude(function () {})
    return page
}

Locker.prototype.checkCacheSize = function (page) {
    var heft = page.items.reduce(function (heft, item) { return heft + item.heft }, 0)
    ok(heft == page.cartridge.heft, 'sizes are wrong')
}

Locker.prototype.unlock = function (page) {
    this.checkCacheSize(page)
    this._locks[page.address].unlock(null, page)
    if (!this._locks[page.address].count) {
        delete this._locks[page.address]
    }
    page.cartridge.release()
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
