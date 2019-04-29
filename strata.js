var path = require('path')
var assert = require('assert')
var fs = require('fs')

var cadence = require('cadence')

var mkdirp = require('mkdirp')

var Cache = require('magazine')

var Appender = require('./appender')
var Cursor = require('./cursor')

var Journalist = require('./journalist')

var Interrupt = require('interrupt').createInterrupter('b-tree')
var Turnstile = require('turnstile')

var find = require('./find')

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

// TODO Branch and leaf size, can we just sort that out in a call to balance?
function Strata (options) {
    this.options = options
    this.options.comparator = options.comparator || compare
    if (!options.serializers) {
        var json = require('./json')
        options.serializers = {
            key: json.serializer,
            record: json.serializer
        }
        options.deserializers = {
            key: json.deserialize,
            record: json.deserialize
        }
    }
    /* if (!options.framer) {
        var UTF8 = require('./frame/utf8')
        options.framer = new UTF8(options.checksum || 'sha1')
    } */
    this.journalist = new Journalist(options)

    this.housekeeper = new Turnstile
    this.writer = new Turnstile
    this._cursors = []
}

Strata.prototype.create = cadence(function (async, options) {
    this.instance = 0
    var directory = this.options.directory
    async(function () {
        fs.stat(directory, async())
    }, function (stat) {
        Interrupt.assert(stat.isDirectory(), 'create.not.directory', { directory: directory })
        fs.readdir(this.journalist.directory, async())
    }, function (files) {
        Interrupt.assert(files.filter(function (f) {
            return ! /^\./.test(f)
        }).length == 0, 'create.directory.not.empty', { directory: directory })
    }, function () {
        mkdirp(path.resolve(directory, 'pages'), 0o755, async())
    }, function () {
        mkdirp(path.resolve(directory, 'instance', '0'), async())
    }, function () {
        async(function () {
            mkdirp(path.resolve(directory, 'pages', '0.0'), 0o755, async())
        }, function () {
            var appender = new Appender(path.resolve(directory, 'pages', '0.0', 'append'))
            async(function () {
                appender.append({ method: 'insert', index: 0, value: { id: '0.1' } }, async())
            }, function () {
                appender.end(async())
            })
        }, function () {
            mkdirp(path.resolve(directory, 'pages', '0.1'), 0o755, async())
        }, function () {
            new Appender(path.resolve(directory, 'pages', '0.1', 'append')).end(async())
        }, function () {
            this.journalist.magazine.hold(-1, { items: [{ id: '0.0' }]  })
        })
    })
})

Strata.prototype.open = cadence(function (async) {
    this.journalist.magazine.hold(-1, { items: [{ id: '0.0' }]  })
    async(function () {
        fs.stat(this.options.directory, async())
    }, function () {
        fs.readdir(path.join(this.options.directory, 'instance'), async())
    }, function (files) {
        files = files.filter(function (file) {
            return /^\d+$/.test(file)
        }).map(function (file) {
            return +file
        }).sort(function (left, right) {
            return right - left
        })
        Interrupt.assert(files.length != 0, 'instance.missing')
        this.instance = files[0] + 1
        async(function () {
            mkdirp(path.resolve(this.options.directory, 'instance', String(this.instance)), async())
        }, function () {
            async.forEach([ files ], function (file) {
                fs.rmdir(path.resolve(this.options.directory, 'instance', String(file)), async())
            })
        })
    })
})

Strata.prototype.close = cadence(function (async) {
    return
    // TODO that's a lot of indirection.
    var cartridge = this.sheaf.metaRoot.cartridge, lock = cartridge.value.page.lock

    lock.unlock()
    // TODO
    lock.dispose()

    cartridge.release()

    var purge = this.sheaf.magazine.purge()
    while (purge.cartridge) {
        purge.cartridge.remove()
        purge.next()
    }
    purge.release()

    assert(!this.sheaf.magazine.count, 'pages still held by cache')
})

Strata.prototype.cursor = cadence(function (async, key, exclusive) {
    // We hold onto all cartridges until we're done, even retries, so we're
    // going to end up holding cartridges two or more times, but we'll make
    // progress eventually and release everything.
    var cartridges = []
    async([function () {
        cartridges.forEach(function (cartridge) { cartridge.release() })
    }], function () {
        async.block(function () {
            var cartridge, index = 0
            cartridges.push(cartridge = this.journalist.magazine.hold(-1, null))
            for (;;) {
                var id = cartridge.value.items[index].id
                cartridges.push(cartridge = this.journalist.magazine.hold(id))
                if (cartridge.value == null) {
                    return async(function () {
                        this.journalist.load(id, async())
                    }, function () {
                        return [ async.continue ]
                    })
                }
                var page = cartridge.value
                index = find(this.options.comparator, cartridge.value, key, page.leaf ? page.ghosts : 1)
                if (page.leaf) {
                    break
                } else if (index < 0) {
                    // On a branch, unless we hit the key exactly, we're
                    // pointing at the insertion point which is right after
                    // the branching we're supposed to decend, so back it up
                    // one unless it's a bullseye.
                    index = ~index - 1
                }
            }
            // Pop the last cartridge to give to the cursor; we don't release it
            // the cursor does.
            return new Cursor(this.journalist, cartridges.pop(), key, index)
        })
    })
})

Strata.prototype.purge = function (downTo) {
    var purge = this.journalist.magazine.purge()
    while (purge.cartridge && this.journalist.magazine.heft > downTo) {
        purge.cartridge.remove()
        purge.next()
    }
    purge.release()
}

module.exports = Strata
