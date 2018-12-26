var Cache = require('magazine'),
    cadence = require('cadence'),
    Cursor = require('./cursor'),
    fs = require('fs'),
    Queue = require('./queue'),
    Script = require('./script'),
    Sheaf = require('./sheaf'),
    Balancer = require('./balancer'),
    Descent = require('./descent'),
    Locker = require('./locker'),
    Player = require('./player'),
    Logger = require('./logger'),
    ok = require('assert').ok,
    path = require('path')

var Interrupt = require('interrupt').createInterrupter('b-tree')
var Turnstile = require('turnstile')

var Journalist = require('./journalist')

// TODO temporary
var scram = require('./scram')

function extend(to, from) {
    for (var key in from) to[key] = from[key]
    return to
}

// TODO Branch and leaf size, can we just sort that out in a call to balance?
function Strata (options) {
    this.options = options
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
    if (!options.framer) {
        var UTF8 = require('./frame/utf8')
        options.framer = new UTF8(options.checksum || 'sha1')
    }
    options.player = new Player(options)
    this.sheaf = options.sheaf = new Sheaf(options)
    this.logger = new Logger(options)

    this.housekeeper = new Turnstile
    this.writer = new Turnstile
    this._sheaf = new Cache().createMagazine()
    this._journalist = new Journalist(options.directory, this._sheaf)
    this._cursors = []
}

Strata.prototype.create = cadence(function (async, options) {
    var directory = this.options.directory
    async(function () {
        fs.stat(directory, async())
    }, function (stat) {
        Interrupt.assert(stat.isDirectory(), 'create.not.directory', { directory: directory })
        fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        Interrupt.assert(files.filter(function (f) {
            return ! /^\./.test(f)
        }).length == 0, 'create.directory.not.empty', { directory: directory })
    }, function () {
        fs.mkdir(path.resolve(directory, 'pages'), 0755, async())
    }, function () {
        fs.writeFile(path.resolve(directory, 'instance'), '0\n', async())
    }, function () {
        async(function () {
            var cartridge = this._journalist.hold([ 'pages', '0' ])
            async([function () {
                cartridge.release()
            }], function () {
                cartridge.value.write(JSON.stringify({ method: 'add', index: 0, value: { id: 1 } }) + '\n', async())
            })
        }, function () {
            this._journalist.close([ 'pages', '0' ], async())
        }, function () {
            this._journalist.hold([ 'pages', '1' ]).release()
            this._journalist.close([ 'pages', '1' ], async())
        }, function () {
            this._sheaf.hold(-1, { items: [{ id: 0 }]  })
        })
    })
})

Strata.prototype.open = cadence(function (async) {
    this.sheaf.createMagazine()

    // TODO instead of rescue, you might try/catch the parts that you know
    // are likely to cause errors and raise an error of a Strata type.

    // TODO or you might need some way to catch a callback error. Maybe an
    // outer most catch block?

    // TODO or if you're using Cadence, doesn't the callback get wrapped
    // anyway?
    async(function () {
        fs.stat(this.sheaf.directory, async())
    }, function stat (error, stat) {
        fs.readdir(path.join(this.sheaf.directory, 'pages'), async())
    }, function (files) {
        files.forEach(function (file) {
            if (/^\d+\.\d+$/.test(file)) {
                this.sheaf.nextAddress = Math.max(+(file.split('.').shift()) + 1, this.sheaf.nextAddress)
            }
        }, this)
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

    ok(!this.sheaf.magazine.count, 'pages still held by cache')
})

Strata.prototype.left = function (descents, exclusive, callback) {
    this.toLeaf(descents[0].left, descents, null, exclusive, callback)
}

Strata.prototype.right = function (descents, exclusive, callback) {
    this.toLeaf(descents[0].right, descents, null, exclusive, callback)
}

Strata.prototype.key = function (key) {
    return function (descents, exclusive, callback) {
        this.toLeaf(descents[0].key(key), descents, null, exclusive, callback)
    }
}

Strata.prototype.leftOf = function (key) {
    return cadence(function (async, descents, exclusive) {
        var conditions = [ descents[0].leaf, descents[0].found([key]) ]
        async(function () {
            descents[0].descend(descents[0].key(key), function () {
                return conditions.some(function (condition) {
                    return condition.call(descents[0])
                })
            }, async())
        }, function (page, index) {
            if (descents[0].page.address % 2) {
                return [ new Cursor(this.sheaf, this.logger, descents, false, key) ]
            } else {
                descents[0].setIndex(descents[0].index - 1)
                this.toLeaf(descents[0].right, descents, null, exclusive, async())
            }
        })
    })
}

Strata.prototype.toLeaf = cadence(function (async, sought, descents, key, exclusive) {
    async(function () {
        descents[0].descend(sought, descents[0].penultimate, async())
    }, function () {
        if (exclusive) descents[0].exclude()
        descents[0].descend(sought, descents[0].leaf, async())
    }, function () {
        return [ new Cursor(this.sheaf, this.logger, descents, exclusive, key) ]
    })
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
            cartridges.push(cartridge = this._sheaf.hold(-1, null))
            for (;;) {
                var id = cartridge.value.items[index].id
                cartridges.push(cartridge = this._sheaf.hold(id))
                if (cartridge.value == null) {
                    return async(function () {
                        this._journalist.load(id, async())
                    }, function () {
                        return [ async.continue ]
                    })
                }
                var page = cartridge.value
                index = this.sheaf.find(cartridge.value, key, page.leaf ? page.ghosts : 1)
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
            return new Cursor(this.sheaf, cartridges.pop(), key, index)
        })
    })
    return
    var descents = [ new Descent(this.sheaf, this.sheaf.createLocker()) ]
    async([function () {
        if (descents.length) {
            descents[0].locker.unlock(descents[0].page)
            descents[0].locker.dispose()
        }
    }], function () {
        if  (typeof key == 'function') {
            key.call(this, descents, exclusive, async())
        } else {
            this.toLeaf(descents[0].key(key), descents, key, exclusive, async())
        }
    }, function (cursor) {
        return [ cursor ]
    })
})

Strata.prototype.iterator = function (key, callback) {
    this.cursor(key, false, callback)
}

Strata.prototype.mutator = function (key, callback) {
    this.cursor(key, true, callback)
}

Strata.prototype.balance = function (callback) {
    new Balancer(this.sheaf, this.logger).balance(callback)
}

Strata.prototype.vivify = cadence(function (async) {
    var locker = this.sheaf.createLocker(), root

    function record (item) {
        return { address: item.address }
    }

    var expand = cadence(function (async, parent, pages, index) {
        async(function () {
            if (index < pages.length) {
                var address = pages[index].address
                async(function () {
                    locker.lock(address, false, async())
                }, [function (page) {
                    locker.unlock(page)
                }])
            } else {
                return [ async.return, pages ]
            }
        }, function (page) {
            if (page.address % 2 == 0) {
                async(function () {
                    pages[index].children = page.items.map(record)
                    if (index) {
                        pages[index].key = parent.items[index].key
                    }
                    expand.call(this, page, pages[index].children, 0, async())
                }, function () {
                    expand.call(this, parent, pages, index + 1, async())
                })
            } else {
                async(function () {
                    pages[index].children = []
                    pages[index].ghosts = page.ghosts

                    for (var i = 0, I = page.items.length; i < I; i++) {
                        pages[index].children.push(page.items[i].record)
                    }
                }, function () {
                    expand.call(this, parent, pages, index + 1, async())
                })
            }
        }, function () {
            return [ pages ]
        })
    })

    async(function () {
        locker.lock(0, false, async())
    }, function (page) {
        async([function () {
            locker.unlock(page)
            locker.dispose()
        }], function () {
            expand.call(this, page, root = page.items.map(record), 0, async())
        })
    })
})

Strata.prototype.purge = function (downTo) {
    var purge = this.sheaf.magazine.purge()
    while (purge.cartridge && this.sheaf.magazine.heft > downTo) {
        purge.cartridge.remove()
        purge.next()
    }
    purge.release()
}

Strata.prototype.__defineGetter__('balanced', function () {
    ok(false)
    return ! Object.keys(this.sheaf.lengths).length
})

module.exports = Strata
