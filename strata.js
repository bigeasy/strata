var Cache = require('magazine'),
    Journalist = require('journalist'),
    cadence = require('cadence/redux'),
    Cursor = require('./cursor'),
    fs = require('fs'),
    Queue = require('./queue'),
    Script = require('./script'),
    Sheaf = require('./sheaf'),
    Descent = require('./descent'),
    Locker = require('./locker'),
    ok = require('assert').ok,
    path = require('path'),
    prototype = require('pointcut').prototype

require('cadence/loops')

// todo: temporary
var scram = require('./scram')

function extend(to, from) {
    for (var key in from) to[key] = from[key]
    return to
}

function Strata (options) {
    this.sheaf = new Sheaf(options)
}

Strata.prototype.__defineGetter__('size', function () {
    return this.sheaf.magazine.heft
})

Strata.prototype.__defineGetter__('nextAddress', function () {
    return this.sheaf.nextAddress
})

// to user land
prototype(Strata, 'create', cadence(function (async) {
    this.sheaf.createMagazine()

    var locker = this.sheaf.createLocker(), count = 0, root, leaf, journal

    var script = new Script(this.sheaf)

    async([function () {
        locker.dispose()
    }], function () {
        this.sheaf.fs.stat(this.sheaf.directory, async())
    }, function (stat) {
        ok(stat.isDirectory(), 'database ' + this.sheaf.directory + ' is not a directory.')
        this.sheaf.fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
              'database ' + this.sheaf.directory + ' is not empty.')

        root = locker.encache(this.sheaf.createBranch({ penultimate: true }))
        leaf = locker.encache(this.sheaf.createLeaf({}))
    }, [function () {
        locker.unlock(root)
        locker.unlock(leaf)
    }], function () {
        this.sheaf.splice(root, 0, 0, { address: leaf.address, heft: 0 })
        script.writeBranch(root)
        script.rewriteLeaf(leaf)
        script.commit(async())
    })
}))

// to user land
prototype(Strata, 'open', cadence(function (async) {
    this.sheaf.createMagazine()

    // todo: instead of rescue, you might try/catch the parts that you know
    // are likely to cause errors and raise an error of a Strata type.

    // todo: or you might need some way to catch a callback error. Maybe an
    // outer most catch block?

    // todo: or if you're using Cadence, doesn't the callback get wrapped
    // anyway?
    async(function () {
        this.sheaf.fs.stat(this.sheaf.directory, async())
    }, function stat (error, stat) {
        this.sheaf.fs.readdir(this.sheaf.directory, async())
    }, function (files) {
        files.forEach(function (file) {
            if (/^\d+\.\d+$/.test(file)) {
                this.sheaf.nextAddress = Math.max(+(file.split('.').shift()) + 1, this.sheaf.nextAddress)
            }
        }, this)
    })
}))

// to user land
prototype(Strata, 'close', cadence(function (async) {
    var cartridge = this.sheaf.magazine.get(-2), lock = cartridge.value.page.lock
    async(function () {
        this.sheaf.createJournal().close('tree', async())
    }, function () {
        lock.unlock()
        // todo
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
}))

prototype(Strata, 'left', function (descents, exclusive, callback) {
    this.toLeaf(descents[0].left, descents, null, exclusive, callback)
})

prototype(Strata, 'right', function (descents, exclusive, callback) {
    this.toLeaf(descents[0].right, descents, null, exclusive, callback)
})

prototype(Strata, 'key', function (key) {
    return function (descents, exclusive, callback) {
        this.toLeaf(descents[0].key(key), descents, null, exclusive, callback)
    }
})

prototype(Strata, 'leftOf', function (key) {
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
                return [ new Cursor(this.sheaf, this.sheaf.createJournal(), descents, false, key) ]
            } else {
                descents[0].index--
                this.toLeaf(descents[0].right, descents, null, exclusive, async())
            }
        })
    })
})

prototype(Strata, 'toLeaf', cadence(function (async, sought, descents, key, exclusive) {
    async(function () {
        descents[0].descend(sought, descents[0].penultimate, async())
    }, function () {
        if (exclusive) descents[0].exclude()
        descents[0].descend(sought, descents[0].leaf, async())
    }, function () {
        return [ new Cursor(this.sheaf, this.sheaf.createJournal(), descents, exclusive, key) ]
    })
}))

// to user land
prototype(Strata, 'cursor', cadence(function (async, key, exclusive) {
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
}))

prototype(Strata, 'iterator', function (key, callback) {
    this.cursor(key, false, callback)
})

prototype(Strata, 'mutator', function (key, callback) {
    this.cursor(key, true, callback)
})

// to user land
prototype(Strata, 'balance', function (callback) {
    this.sheaf.balance(callback)
})

// to user land
prototype(Strata, 'vivify', cadence(function (async) {
    var locker = this.sheaf.createLocker(), root

    function record (item) {
        return { address: item.address }
    }

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

    var expand = cadence(function (async, parent, pages, index) {
        var block = async(function () {
            if (index < pages.length) {
                var address = pages[index].address
                async(function () {
                    locker.lock(address, false, async())
                }, [function (page) {
                    locker.unlock(page)
                }])
            } else {
                return [ block, pages ]
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
            return [ block, pages ]
        })()
    })
}))

Strata.prototype.purge = function (downTo) {
    var purge = this.sheaf.magazine.purge()
    while (purge.cartridge && this.sheaf.magazine.heft > downTo) {
        purge.cartridge.remove()
        purge.next()
    }
    purge.release()
}

Strata.prototype.__defineGetter__('balanced', function () {
    return ! Object.keys(this.sheaf.lengths).length
})

module.exports = Strata
