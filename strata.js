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

// TODO temporary
var scram = require('./scram')

function extend(to, from) {
    for (var key in from) to[key] = from[key]
    return to
}

// TODO Branch and leaf size, can we just sort that out in a call to balance?
function Strata (options) {
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
}

Strata.prototype.create = cadence(function (async) {
    this.sheaf.createMagazine()
    var locker = this.sheaf.createLocker(), count = 0, root, leaf, journal
    async([function () {
        locker.dispose()
    }], function () {
        fs.stat(this.sheaf.directory, async())
    }, function (stat) {
        ok(stat.isDirectory(), 'database ' + this.sheaf.directory + ' is not a directory.')
    }, function () {
        fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
              'database ' + this.sheaf.directory + ' is not empty.')
        this.logger.mkdir(async())
    }, function () {
        root = locker.encache(this.sheaf.createPage(0))
        leaf = locker.encache(this.sheaf.createPage(1))
    }, [function () {
        locker.unlock(root)
        locker.unlock(leaf)
    }], function () {
        var script = this.logger.createScript()
        root.splice(0, 0, { address: leaf.address, heft: 0 })
        script.writeBranch(root)
        script.rewriteLeaf(leaf)
        script.commit(async())
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
                    console.log('foo', page)
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
