var Cache = require('magazine'),
    Journalist = require('journalist'),
    Rescue = require('rescue'),
    cadence = require('cadence')

// todo: temporary
var scram = require('./scram')

function extend(to, from) {
    for (var key in from) to[key] = from[key]
    return to
}

var __slice = [].slice

/*function say() {
        var args = __slice.call(arguments)
        console.log(require('util').inspect(args, false, null))
}*/

function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

function extract (a) { return a }

function classify () {
    var i, I, name
    for (i = 0, I = arguments.length; i < I; i++) {
        name = arguments[i].name
        if (name[0] == '_')
            this.__defineGetter__(name.slice(1), arguments[i])
        else if (name[name.length - 1] == '_')
            this.__defineSetter__(name.slice(0, name.length - 1), arguments[i])
        else
            this[arguments[i].name] = arguments[i]
    }
    return this
}

function Strata (options) {
    var writeFooter = cadence(function (step, out, position, page) {
        ok(page.address % 2 && page.bookmark != null)
        var header = [
            0, page.bookmark.position, page.bookmark.length, page.bookmark.entry,
            page.right || 0, page.position, page.entries, page.ghosts, page.positions.length - page.ghosts
        ]
        step(function () {
            writeEntry({
                out: out,
                page: page,
                header: header,
                type: 'footer'
            }, step())
        }, function (position, length) {
            page.position = header[5] // todo: can't we use `position`?
            return [ position, length ]
        })
    })

    var sequester = options.sequester || require('sequester'),
        directory = options.directory,
        extractor = options.extractor || extract,
        comparator = options.comparator || compare,
        fs = options.fs || require('fs'),
        path = options.path || require('path'),
        ok = function (condition, message) { if (!condition) throw new Error(message) },
        cache = options.cache || (new Cache),
        thrownByUser,
        magazine,
        nextAddress = 0,
        length = 1024,
        balancer = new Balancer(),
        balancing,
        size = 0,
        checksum,
        constructors = {},
        journal = {
            branch: new Journalist({ stage: 'entry' }).createJournal(),
            leaf: new Journalist({
                stage: 'entry',
                closer: writeFooter
            }).createJournal()
        },
        journalist = new Journalist({
            count: options.fileHandleCount || 64,
            stage: options.writeStage || 'entry',
            cache: options.jouralistCache || (new Cache),
            closer: writeFooter
        }),
        createJournal = (options.writeStage == 'tree' ? (function () {
            var journal = journalist.createJournal()
            return function () { return journal }
        })() : function () {
            return journalist.createJournal()
        }),
        serialize = options.serialize || function (object) { return new Buffer(JSON.stringify(object)) },
        deserialize = options.deserialize || function (buffer) { return JSON.parse(buffer.toString()) },
        tracer = options.tracer || function () { arguments[2]() },
        rescue = new Rescue

    checksum = (function () {
        if (typeof options.checksum == 'function') return options.checksum
        var algorithm
        switch (algorithm = options.checksum || 'sha1') {
        case 'none':
            return function () {
                return {
                    update: function () {},
                    digest: function () { return '0' }
                }
            }
        default:
            var crypto = require('crypto')
            return function (m) { return crypto.createHash(algorithm) }
        }
    })()

    function validate (callback, forward, janitor) {
        return rescue.validate(callback, forward, janitor)
    }

    function _size () { return magazine.heft }

    function _nextAddress () { return nextAddress }

    function readEntry (buffer, isKey) {
        for (var count = 2, i = 0, I = buffer.length; i < I && count; i++) {
            if (buffer[i] == 0x20) count--
        }
        for (count = 1; i < I && count; i++) {
            if (buffer[i] == 0x20 || buffer[i] == 0x0a) count--
        }
        ok(!count, 'corrupt line: could not find end of line header')
        var fields = buffer.toString('utf8', 0, i - 1).split(' ')
        var hash = checksum(), body, length
        hash.update(fields[2])
        if (buffer[i - 1] == 0x20) {
            body = buffer.slice(i, buffer.length - 1)
            length = body.length
            hash.update(body)
        }
        var digest = hash.digest('hex')
        ok(fields[1] == '-' || digest == fields[1], 'corrupt line: invalid checksum')
        if (buffer[i - 1] == 0x20) {
            body = deserialize(body, isKey)
        }
        var entry = { length: length, header: JSON.parse(fields[2]), body: body }
        ok(entry.header.every(function (n) { return typeof n == 'number' }), 'header values must be numbers')
        return entry
    }

    function filename (address, suffix) {
        suffix || (suffix = '')
        return path.join(directory, address + suffix)
    }

    var replace = cadence(function (step, page, suffix) {
        var replacement = filename(page.address, suffix),
            permanent = filename(page.address)

        step(function () {
            fs.stat(replacement, step())
        }, function (stat) {
            ok(stat.isFile(), 'is not a file')
            step([function () {
                fs.unlink(permanent, step())
            }, /^ENOENT$/, function () {
                // todo: regex only is a catch and swallow?
            }])
        }, function (ror) {
            fs.rename(replacement, permanent, step())
        })
    })

    function rename (page, from, to, callback) {
        fs.rename(filename(page.address, from), filename(page.address, to), callback)
    }

    function unlink (page, suffix, callback) {
        fs.unlink(filename(page.address, suffix), callback)
    }

    function heft (page, s) {
        magazine.get(page.address).adjustHeft(s)
    }

    function createLeaf (override) {
        return createPage({
            cache: {},
            loaders: {},
            entries: 0,
            ghosts: 0,
            positions: [],
            lengths: [],
            right: 0,
            queue: sequester.createQueue()
        }, override, 0)
    }

    constructors.leaf = createLeaf

    function _cacheRecord (page, position, record, length) {
        var key = extractor(record)
        ok(key != null, 'null keys are forbidden')

        var entry = {
            record: record,
            size: length,
            key: key,
            keySize: serialize(key, true).length
        }

        return encacheEntry(page, position, entry)
    }

    function encacheEntry (page, reference, entry) {
        ok (!page.cache[reference], 'record already cached for position')

        page.cache[reference] = entry

        heft(page, entry.size)

        return entry
    }

    function uncacheEntry (page, reference) {
        var entry = page.cache[reference]
        ok (entry, 'entry not cached')
        heft(page, -entry.size)
        delete page.cache[reference]
        return entry
    }

    var writeEntry = cadence(function (step, options) {
        var entry, buffer, json, line, length

        ok(options.page.position != null, 'page has not been positioned: ' + options.page.position)
        ok(options.header.every(function (n) { return typeof n == 'number' }), 'header values must be numbers')

        if (options.type == 'position') {
            options.page.bookmark = { position: options.page.position }
        }

        entry = options.header.slice()
        json = JSON.stringify(entry)
        var hash = checksum()
        hash.update(json)

        length = 0

        var separator = ''
        if (options.body != null) {
            var body = serialize(options.body, options.isKey)
            separator = ' '
            length += body.length
            hash.update(body)
        }

        line = hash.digest('hex') + ' ' + json + separator

        length += Buffer.byteLength(line, 'utf8') + 1

        var entire = length + String(length).length + 1
        if (entire < length + String(entire).length + 1) {
            length = length + String(entire).length + 1
        } else {
            length = entire
        }

        buffer = new Buffer(length)
        buffer.write(String(length) + ' ' + line)
        if (options.body != null) {
            body.copy(buffer, buffer.length - 1 - body.length)
        }
        buffer[length - 1] = 0x0A

        if (options.type == 'position') {
            options.page.bookmark.length = length
            options.page.bookmark.entry = entry[0]
        }

        var position = options.page.position

        step(function () {
            options.out.write(buffer, step())
        }, function () {
            options.page.position += length
            return [ position, length, body && body.length ]
        })
    })

    function writeInsert (out, page, index, record, callback) {
        var header = [ ++page.entries, index + 1 ]
        writeEntry({ out: out, page: page, header: header, body: record }, callback)
    }

    function writeDelete (out, page, index, callback) {
        var header = [ ++page.entries, -(index + 1) ]
        writeEntry({ out: out, page: page, header: header }, callback)
    }

    var io = cadence(function (step, direction, filename) {
        step(function () {
            fs.open(filename, direction[0], step())
        }, function (fd) {
            step(function () {
                fs.fstat(fd, step())
            }, function (stat) {
                var io = cadence(function (step, buffer, position) {
                    var offset = 0

                    var length = stat.size - position
                    var slice = length < buffer.length ? buffer.slice(0, length) : buffer

                    var loop = step(function (count) {
                        if (count < slice.length - offset) {
                            offset += count
                            fs[direction](fd, slice, offset, slice.length - offset, position + offset, step())
                        } else {
                            return [ loop, slice, position ]
                        }
                    })(null, 0)
                })
                return [ fd, stat, io ]
            })
        })
    })

    function writePositions (output, page, callback) {
        var header = [ ++page.entries, 0, page.ghosts ]
        header = header.concat(page.positions).concat(page.lengths)
        writeEntry({ out: output, page: page, header: header, type: 'position' }, callback)
    }

    function readHeader (entry) {
        var header = entry.header
        return {
            entry:      header[0],
            index:      header[1],
            address:    header[2]
        }
    }

    function readFooter (entry) {
        var footer = entry.header
        return {
            entry:      footer[0],
            bookmark: {
                position:   footer[1],
                length:     footer[2],
                entry:      footer[3]
            },
            right:      footer[4],
            position:   footer[5],
            entries:    footer[6],
            ghosts:     footer[7],
            records:    footer[8]
        }
    }

    var findPositionsArray = cadence(function (step, page, fd, stat, read) {
        var positions = [],
            lengths = [],
            bookmark
        var buffer = new Buffer(options.readLeafStartLength || 1024)
        step(function () {
            read(buffer, Math.max(0, stat.size - buffer.length), step())
        }, function (slice) {
            for (var i = slice.length - 2; i != -1; i--) {
                if (slice[i] == 0x0a) {
                    var footer = readFooter(readEntry(slice.slice(i + 1)))
                    ok(!footer.entry, 'footer is supposed to be zero')
                    bookmark = footer.bookmark
                    page.right = footer.right
                    page.position = footer.position
                    ok(page.position != null, 'no page position')
                    read(new Buffer(bookmark.length), bookmark.position, step())
                    return
                }
            }
            throw new Error('cannot find footer in last ' + buffer.length + ' bytes')
        }, function (slice) {
            var positions = readEntry(slice.slice(0, bookmark.length)).header

            page.entries = positions.shift()
            ok(page.entries == bookmark.entry, 'position entry number incorrect')
            ok(positions.shift() == 0, 'expected housekeeping type')

            page.ghosts = positions.shift()

            ok(!(positions.length % 2), 'expecting even number of positions and lengths')
            var lengths = positions.splice(positions.length / 2)

            splice('positions', page, 0, 0, positions)
            splice('lengths', page, 0, 0, lengths)

            page.bookmark = bookmark

            return [ page, bookmark.position + bookmark.length ]
        })
    })

    var readLeaf = cadence(function (step, page) {
        step(function () {
            io('read', filename(page.address), step())
        }, function (fd, stat, read) {
            step(function () {
                if (options.replay) {
                    page.entries = 0
                    page.ghosts = 0
                    return [ page, 0 ]
                } else {
                    findPositionsArray(page, fd, stat, read, step())
                }
            }, function (page, position) {
                replay(fd, stat, read, page, position, step())
            }, function () {
                return [ page ]
            })
        })
    })

    var replay = cadence(function (step, fd, stat, read, page, position) {
        var leaf = !!(page.address % 2),
            seen = {},
            buffer = new Buffer(options.readLeafStartLength || 1024),
            footer

        // todo: really want to register a cleanup without an indent.
        step([function () {
            fs.close(fd, step())
        }], function () {
            var loop = step(function (buffer, position) {
                read(buffer, position, step())
            }, function (slice, start) {
                for (var offset = 0, i = 0, I = slice.length; i < I; i++) {
                    ok(!footer, 'data beyond footer')
                    if (slice[i] == 0x20) {
                        var sip = slice.toString('utf8', offset, i)
                        length = parseInt(sip)
                        ok(String(length).length == sip.length, 'invalid length')
                        if (offset + length > slice.length) {
                            break
                        }
                        var position = start + offset
                        ok(length)
                        var entry = readEntry(slice.slice(offset, offset + length), !leaf)
                        var header = readHeader(entry)
                        if (header.entry) {
                            ok(header.entry == ++page.entries, 'entry count is off')
                            var index = header.index
                            if (leaf) {
                                if (index > 0) {
                                    seen[position] = true
                                    splice('positions', page, index - 1, 0, position)
                                    splice('lengths', page, index - 1, 0, length)
                                    _cacheRecord(page, position, entry.body, entry.length)
                                } else if (~index == 0 && page.address != 1) {
                                    ok(!page.ghosts, 'double ghosts')
                                    page.ghosts++
                                } else if (index < 0) {
                                    var outgoing = splice('positions', page, -(index + 1), 1).shift()
                                    if (seen[outgoing]) uncacheEntry(page, outgoing)
                                    splice('lengths', page, -(index + 1), 1)
                                } else {
                                    page.bookmark = {
                                        position: position,
                                        length: length,
                                        entry: header.entry
                                    }
                                }
                            } else {
                                /* if (index > 0) { */
                                    var address = header.address
                                    splice('addresses', page, index - 1, 0, address)
                                    if (index - 1) {
                                        encacheKey(page, address, entry.body, entry.length)
                                    }
                                /* } else {
                                    var cut = splice('addresses', page, ~index, 1)
                                    if (~index) {
                                        uncacheEntry(page, cut[0])
                                    }
                                } */
                            }
                        } else {
                            footer = readFooter(entry)
                            page.position = position
                            page.right = footer.right
                        }
                        i = offset = offset + length
                    }
                }

                if (start + buffer.length < stat.size) {
                    if (offset == 0) {
                        buffer = new Buffer(buffer.length * 2)
                        read(buffer, start, step())
                    } else {
                        read(buffer, start + offset, step())
                    }
                } else {
                    return [ loop, page, footer ]
                }
            })(null, buffer, position)
        })
    })

    var readRecord = cadence(function (step, page, position, length) {
        step(function () {
            io('read', filename(page.address), step())
        }, function (fd, stat, read) {
            step([function () {
                // todo: test what happens when a finalizer throws an error
                fs.close(fd, step())
            }],function () {
                tracer('readRecord', { page: page }, step())
            }, function () {
                read(new Buffer(length), position, step())
            }, function (buffer) {
                ok(buffer[length - 1] == 0x0A, 'newline expected')
                return [ readEntry(buffer, false) ]
            })
        })
    })

    var rewriteLeaf = cadence(function (step, page, suffix) {
        var cache = {}, index = 0, out

        step(function () {
            out = journal.leaf.open(filename(page.address, suffix), 0, page)
            out.ready(step())
        }, [function () {
            // todo: ensure that cadence finalizers are registered in order.
            // todo: also, don't you want to use a specific finalizer above?
            // todo: need an error close!
            out.scram(step())
        }], function () {
            page.position = 0
            page.entries = 0

            var positions = splice('positions', page, 0, page.positions.length)
            var lengths = splice('lengths', page, 0, page.lengths.length)

            step(function () {
                writePositions(out, page, step())
            }, function () {
                step(function (position) {
                    var length = lengths.shift()
                    step(function () {
                        stash(page, position, length, step())
                    }, function (entry) {
                        step(function () {
                            uncacheEntry(page, position)
                            writeInsert(out, page, index++, entry.record, step())
                        }, function (position, length) {
                            cache[position] = entry
                            splice('positions', page, page.positions.length, 0, position)
                            splice('lengths', page, page.lengths.length, 0, length)
                        })
                    })
                })(positions)
            })
        }, function () {
            if (page.positions.length) {
                var entry
                for (var position in cache) {
                    entry = cache[position]
                    encacheEntry(page, position, entry)
                }
                writePositions(out, page, step())
            }
        }, function () {
            out.close('entry', step())
        })
    })

    function createPage (page, override, remainder) {
        if (override.address == null) {
            while ((nextAddress % 2) == remainder) nextAddress++
            override.address = nextAddress++
        }
        return extend(page, override)
    }

    function createBranch (override) {
        return createPage({
            addresses: [],
            cache: {},
            entries: 0,
            penultimate: true,
            queue: sequester.createQueue()
        }, override, 1)
    }

    constructors.branch = createBranch

    function splice (collection, page, offset, length, insert) {
        ok(typeof collection == 'string', 'incorrect collection passed to splice')

        var values = page[collection], json, removals

        ok(values, 'incorrect collection passed to splice')

        if (length) {
            removals = values.splice(offset, length)

            json = values.length == 0 ? '[' + removals.join(',') + ']'
                                                                : ',' + removals.join(',')

            heft(page, -json.length)
        } else {
            removals = []
        }

        if (insert != null) {
            if (! Array.isArray(insert)) insert = [ insert ]
            if (insert.length) {
                json = values.length == 0 ? '[' + insert.join(',') + ']'
                                                                    : ',' + insert.join(',')

                heft(page, json.length)

                values.splice.apply(values, [ offset, 0 ].concat(insert))
            }
        }
        return removals
    }

    function encacheKey (page, address, key, length) {
        return encacheEntry(page, address, { key: key, size: length })
    }

    var writeBranch = cadence(function (step, page, suffix) {
        var keys = page.addresses.map(function (address, index) {
                return page.cache[address]
            }),
            out

        ok(keys[0] === (void(0)), 'first key is null')
        ok(keys.slice(1).every(function (key) { return key != null }), 'null keys')

        step(function () {
            page.entries = 0
            page.position = 0

            out = journal.branch.open(filename(page.address, suffix), 0, page)
            out.ready(step())
        }, [function () {
            out.scram(step())
        }], function () {
            step(function (address) {
                var key = page.entries ? page.cache[address].key : null
                page.entries++
                var header = [ page.entries, page.entries, address ]
                writeEntry({
                    out: out,
                    page: page,
                    header: header,
                    body: key,
                    isKey: true
                }, step())
            })(page.addresses)
        }, function () {
            out.close('entry', step())
        })
    })

    var readBranch = cadence(function (step, page) {
        step(function () {
            io('read', filename(page.address), step())
        }, function (fd, stat, read) {
            replay(fd, stat, read, page, 0, step())
        })
    })

    function createMagazine () {
        var magazine = cache.createMagazine()
        var dummy = magazine.hold(-2, {
            page: {
                address: -2,
                addresses: [ 0 ],
                queue: sequester.createQueue()
            }
        }).value.page
        dummy.lock = dummy.queue.createLock()
        dummy.lock.share(function () {})
        return magazine
    }

    // to user land
    var create = cadence(function (step) {
        var locker = new Locker, count = 0, root, leaf, journal

        magazine = createMagazine()

        step([function () {
            locker.dispose()
        }], function () {
            fs.stat(directory, step())
        }, function (stat) {
            ok(stat.isDirectory(), 'database ' + directory + ' is not a directory.')
            fs.readdir(directory, step())
        }, function (files) {
            ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
                  'database ' + directory + ' is not empty.')

            root = locker.encache(createBranch({ penultimate: true }))
            leaf = locker.encache(createLeaf({}))
            splice('addresses', root, 0, 0, leaf.address)

            writeBranch(root, '.replace', step())
        }, [function () {
            locker.unlock(root)
        }], function () {
            rewriteLeaf(leaf, '.replace', step())
        }, [function () {
            locker.unlock(leaf)
        }], function () {
            replace(root, '.replace', step())
        }, function branchReplaced () {
            replace(leaf, '.replace', step())
        })
    })

    // to user land
    var open = cadence(function (step) {
        magazine = createMagazine()

        // todo: instead of rescue, you might try/catch the parts that you know
        // are likely to cause errors and raise an error of a Strata type.

        // todo: or you might need some way to catch a callback error. Maybe an
        // outer most catch block?

        // todo: or if you're using Cadence, doesn't the callback get wrapped
        // anyway?
        step(function () {
            fs.stat(directory, step())
        }, function stat (error, stat) {
            fs.readdir(directory, step())
        }, function (files) {
            files.forEach(function (file) {
                if (/^\d+$/.test(file)) {
                    nextAddress = Math.max(+(file) + 1, nextAddress)
                }
            })
        })
    })

    // to user land
    var close = cadence(function (step) {
        var cartridge = magazine.get(-2), lock = cartridge.value.page.lock
        step(function () {
            createJournal().close('tree', step())
        }, function () {
            lock.unlock()
            // todo
            lock.dispose()

            cartridge.release()

            var purge = magazine.purge()
            while (purge.cartridge) {
                purge.cartridge.remove()
                purge.next()
            }
            purge.release()

            ok(!magazine.count, 'pages still held by cache')
        })
    })

    var stash = cadence(function (step, page, positionOrIndex, length) {
        var position = positionOrIndex
        if (arguments.length == 3) {
            position = page.positions[positionOrIndex]
            length = page.lengths[positionOrIndex]
        }
        ok(length)
        var entry, loader
        if (loader = page.loaders[position]) {
            loader.share(step())
        } else if (!(entry = page.cache[position])) {
            loader = page.loaders[position] = sequester.createLock()
            loader.exclude(function () {
                readRecord(page, position, length, function (error, entry) {
                    delete page.loaders[position]
                    if (!error) {
                        delete page.cache[position]
                        var entry = _cacheRecord(page, position, entry.body, entry.length)
                    }
                    loader.unlock(error, entry, length)
                })
            })
            stash(page, position, length, step())
        } else {
            return [ entry, length ]
        }
    })

    var _find = cadence(function (step, page, key, low) {
        var mid, high = (page.addresses || page.positions).length - 1

        if (page.address % 2 == 0) {
            while (low <= high) {
                mid = low + ((high - low) >>> 1)
                var compare = comparator(key, page.cache[page.addresses[mid]].key)
                if (compare < 0) high = mid - 1
                else if (compare > 0) low = mid + 1
                else return mid
            }
            return [ ~low ]
        }

        var loop = step(function () {
            if (low <= high) {
                mid = low + ((high - low) >>> 1)
                stash(page, mid, step())
            } else {
                return [ loop, ~low ]
            }
        }, function (entry) {
            ok(entry.key != null, 'key is null in find')
            var compare = comparator(key, entry.key)
            if (compare == 0) {
                return [ loop, mid ]
            } else {
                if (compare > 0) low = mid + 1
                else high = mid - 1
            }
        })()
    })

    function Locker () {
        var locks ={}

        var lock = cadence(function (step, address, exclusive) {
            var cartridge = magazine.hold(address, {}), page = cartridge.value.page, locked

            ok(!locks[address], 'address already locked by this locker')

            if (!page)  {
                page = cartridge.value.page = constructors[address % 2 ? 'leaf' : 'branch']({ address: address })
                locks[page.address] = page.queue.createLock()
                locks[page.address].exclude(function () {
                    if (page.address % 2) {
                        readLeaf(page, loaded)
                    } else {
                        readBranch(page, loaded)
                    }
                    function loaded (error) {
                        if (error) {
                            cartridge.value.page = null
                            cartridge.adjustHeft(-cartridge.heft)
                        }
                        locks[page.address].unlock(error, page)
                    }
                })
            } else {
                locks[page.address] = page.queue.createLock()
            }

            step([function () {
                step(function () {
                    locks[page.address][exclusive ? 'exclude' : 'share'](step())
                },
                function () {
                    tracer('lock', { address: address, exclusive: exclusive }, step())
                }, function () {
                    locked = true
                    return [ page ]
                })
            }, function (errors, error) {
                // todo: if you don't return something, then the return is the
                // error, but what else could it be? Document that behavior, or
                // set a reasonable default.
                magazine.get(page.address).release()
                locks[page.address].unlock(error)
                delete locks[page.address]
                throw errors
            }])
        })

        function encache (page) {
            magazine.hold(page.address, { page: page })
            locks[page.address] = page.queue.createLock()
            locks[page.address].exclude(function () {})
            return page
        }

        function checkCacheSize (page) {
            var size = 0, position
            if (page.address != -2) {
                if (page.address % 2) {
                    if (page.positions.length) {
                        size += JSON.stringify(page.positions).length
                        size += JSON.stringify(page.lengths).length
                    }
                } else {
                    if (page.addresses.length) {
                        size += JSON.stringify(page.addresses).length
                    }
                }
                for (position in page.cache) {
                    size += page.cache[position].size
                }
            }
            ok(size == magazine.get(page.address).heft, 'sizes are wrong')
        }

        function unlock (page) {
            checkCacheSize(page)
            locks[page.address].unlock(null, page)
            if (!locks[page.address].count) {
                delete locks[page.address]
            }
            magazine.get(page.address).release()
        }

        function increment (page) {
            locks[page.address].increment()
            magazine.hold(page.address)
        }

        function dispose () {
            ok(!Object.keys(locks).length, 'locks outstanding')
            locks = null
        }

        classify.call(this, lock, encache, increment, unlock, dispose)

        this.lock = lock

        return this
    }

    function Descent (locker, override) {
        override = override || {}

        var exclusive = override.exclusive || false,
            depth = override.depth == null ? -1 : override.depth,
            index = override.index == null ? 0 : override.index,
            page = override.page,
            indexes = override.indexes || {},
            descent = {},
            greater = override.greater, lesser = override.lesser,
            called

        if (!page) {
            locker.lock(-2, false, function (error, $page) {
                ok(!error, 'impossible error')
                page = $page
            })
            ok(page, 'dummy page not in cache')
        } else {
            locker.increment(page)
        }

        function _locker () { return locker }

        function _page () { return page }

        function _index () { return index }

        function index_ (i) { indexes[page.address] = index = i }

        function _indexes () { return indexes }

        function _depth () { return depth }

        function _lesser () { return lesser }

        function _greater () { return greater }

        function fork () {
            return new Descent(locker, {
                page: page,
                exclusive: exclusive,
                depth: depth,
                index: index,
                indexes: extend({}, indexes)
            })
        }

        function exclude () { exclusive = true }

        var upgrade = cadence(function (step) {
            step([function () {
                locker.unlock(page)
                locker.lock(page.address, exclusive = true, step())
            }, function (errors) {
                locker.lock(-2, false, function (error, locked) {
                    ok(!error, 'impossible error')
                    page = locked
                })
                ok(page, 'dummy page not in cache')
                throw errors
            }], function (locked) {
                page = locked
            })
        })

        function key (key) {
            return function (callback) {
                var found = _find(page, key, page.address % 2 ? page.ghosts : 1, callback)
                return found
            }
        }

        function left (callback) { callback(null, page.ghosts || 0) }

        function right (callback) { callback(null, (page.addresses || page.positions).length - 1) }

        function found (keys) {
            return function () {
                return page.addresses[0] != 0 && index != 0 && keys.some(function (key) {
                    return comparator(page.cache[page.addresses[index]].key,  key) == 0
                })
            }
        }

        function child (address) { return function () { return page.addresses[index] == address } }

        function address (address) { return function () { return page.address == address } }

        function penultimate () { return page.addresses[0] % 2 }

        function leaf () { return page.address % 2 }

        function level (level) {
            return function () { return level == depth }
        }

        function unlocker (parent) {
            locker.unlock(parent)
        }

        function unlocker_ ($unlocker) { unlocker = $unlocker }

        var descend = cadence(function (step, next, stop) {
            var above = page

            var loop = step(function () {
                if (stop()) {
                    return [ loop, page, index ]
                } else {
                    if (index + 1 < page.addresses.length) {
                        greater = page.address
                    }
                    if (index > 0) {
                        lesser = page.address
                    }
                    locker.lock(page.addresses[index], exclusive, step())
                }
            }, function (locked) {
                depth++
                unlocker(page, locked)
                page = locked
                next(step())
            }, function ($index) {
                if (!(page.address % 2) && $index < 0) {
                    index = (~$index) - 1
                } else {
                    index = $index
                }
                indexes[page.address] = index
                if (!(page.address % 2)) {
                    ok(page.addresses.length, 'page has addresses')
                    ok(page.cache[page.addresses[0]] == (void(0)), 'first key is cached')
                }
            })()
        })

        classify.call(this, descend, fork, exclude, upgrade,
                                   key, left, right,
                                   found, address, child, penultimate, leaf, level,
                                   _locker, _page, _depth, _index, index_, _indexes, _lesser, _greater,
                                   unlocker_)
        this.upgrade = upgrade
        this.descend = descend
        return this
    }

    function Cursor (journal, descents, exclusive, searchKey) {
        var locker = descents[0].locker,
            page = descents[0].page,
            rightLeafKey = null,
            length = page.positions.length,
            index = descents[0].index,
            offset = index < 0 ? ~ index : index

        descents.shift()

        // to user land
        function get (index, callback) {
            stash(page, index, validate(callback, function (entry, size) {
                callback(null, entry.record, entry.key, size)
            }))
        }

        // to user land
        var next = cadence(function (step) {
            var next
            rightLeafKey = null

            if (!page.right) {
                // return [ step, false ] <- return immediately!
                return [ false ]
            }

            step(function () {
                locker.lock(page.right, exclusive, step())
            }, function (next) {
                locker.unlock(page)

                page = next

                offset = page.ghosts
                length = page.positions.length

                return [ true ]
            })
        })

        function indexOf (key, callback) {
            _find(page, key, page.ghosts, callback)
        }

        function unlock (callback) {
            ok(callback, 'unlock now requires a callback')

            journal.close('leaf', validate(callback, unlock))

            function unlock () {
                locker.unlock(page)
                locker.dispose()
                callback()
            }
        }

        function _index () { return index }

        function _offset () { return offset }

        function _length () { return length }

        function _ghosts () { return page.ghosts }

        function _address () { return page.address }

        function _right () { return page.right }

        function _exclusive () { return exclusive }

        classify.call(this, unlock, indexOf, get, next,
                            _index, _offset, _length, _ghosts, _address, _right, _exclusive)
        this.next = next

        if (!exclusive) return this

        // to user land
        var insert = cadence(function (step, record, key, index) {
            var unambiguous

            var block = step(function () {
                if (index == 0 && page.address != 1) {
                    return [ block, -1 ]
                }

                unambiguous = index < page.positions.length
                unambiguous = unambiguous || searchKey.length && comparator(searchKey[0], key) == 0
                unambiguous = unambiguous || ! page.right

                if (!unambiguous) step(function () {
                    if (!rightLeafKey) step(function () {
                        locker.lock(page.right, false, step())
                    }, function (rightLeafPage) {
                        step(function () {
                            stash(rightLeafPage, 0, step())
                        }, [function () {
                            locker.unlock(rightLeafPage)
                        }], function (entry) {
                            rightLeafKey = entry.key
                        })
                    })
                }, function  () {
                    if (comparator(key, rightLeafKey) >= 0) {
                        return [ block, +1 ]
                    }
                })
            }, function () {
                var entry
                balancer.unbalanced(page)
                step(function () {
                    entry = journal.open(filename(page.address), page.position, page)
                    journalist.purge(step())
                }, function () {
                    entry.ready(step())
                }, function () {
                    scram(entry, cadence(function (step) {
                        step(function () {
                            writeInsert(entry, page, index, record, step())
                        }, function (position, length, size) {
                            splice('positions', page, index, 0, position)
                            splice('lengths', page, index, 0, length)
                            _cacheRecord(page, position, record, size)

                            length = page.positions.length
                        }, function () {
                            step(function () {
                                entry.close('entry', step())
                            }, function () {
                                return [ 0 ]
                            })
                        })
                    }), step())
                })
            })(1)
        })

        var remove = cadence(function (step, index) {
            var ghost = page.address != 1 && index == 0, entry
            balancer.unbalanced(page)
            step(function () {
                journalist.purge(step())
            }, function () {
                entry = journal.open(filename(page.address), page.position, page)
                entry.ready(step())
            }, function () {
                scram(entry, cadence(function (step) {
                    step(function () {
                        writeDelete(entry, page, index, step())
                    }, function () {
                        if (ghost) {
                            page.ghosts++
                            offset || offset++
                        } else {
                            uncacheEntry(page, page.positions[index])
                            splice('positions', page, index, 1)
                            splice('lengths', page, index, 1)
                        }
                    }, function () {
                        entry.close('entry', step())
                    })
                }), step())
            })
        })

        classify.call(this, insert, remove)
        this.insert = insert
        this.remove = remove
        return this
    }

    function Balancer () {
        var lengths = {},
            operations = [],
            referenced = {},
            ordered = {},
            ghosts = {},
            methods = {}

        function unbalanced (page, force) {
            if (force) {
                lengths[page.address] = options.leafSize
            } else if (lengths[page.address] == null) {
                lengths[page.address] = page.positions.length - page.ghosts
            }
        }

        var _nodify = cadence(function (step, locker, page) {
            step(function () {
                step([function () {
                    locker.unlock(page)
                }], function () {
                    ok(page.address % 2, 'leaf page expected')

                    if (page.address == 1) return [{}]
                    else stash(page, 0, step())
                }, function (entry) {
                    var node = {
                        key: entry.key,
                        address: page.address,
                        rightAddress: page.right,
                        length: page.positions.length - page.ghosts
                    }
                    ordered[node.address] = node
                    if (page.ghosts) {
                        ghosts[node.address] = node
                    }
                    return [ node ]
                })
            }, function (node) {
                step(function () {
                    tracer('reference', {}, step())
                }, function () {
                    return node
                })
            })
        })

        var balance = cadence(function balance (step) {
            var locker = new Locker, address

            var _gather = cadence(function (step, address, length) {
                var right, node
                step(function () {
                    if (node = ordered[address]) {
                        return [ node ]
                    } else {
                        step(function () {
                            locker.lock(address, false, step())
                        }, function (page) {
                            _nodify(locker, page, step())
                        })
                    }
                }, function (node) {
                    if (!(node.length - length < 0)) return
                    if (node.address != 1 && ! node.left) step(function () {
                        var descent = new Descent(locker)
                        step(function () {
                            descent.descend(descent.key(node.key), descent.found([node.key]), step())
                        }, function () {
                            descent.index--
                            descent.descend(descent.right, descent.leaf, step())
                        }, function () {
                            if (left = ordered[descent.page.address]) {
                                locker.unlock(descent.page)
                                return [ left ]
                            } else {
                                _nodify(locker, descent.page, step())
                            }
                        }, function (left) {
                            left.right = node
                            node.left = left
                        })
                    })
                    if (!node.right && node.rightAddress) step(function () {
                        if (right = ordered[node.rightAddress]) return [ right ]
                        else step(function () {
                            locker.lock(node.rightAddress, false, step())
                        }, function (page) {
                            _nodify(locker, page, step())
                        })
                    }, function (right) {
                        node.right = right
                        right.left = node
                    })
                })
            })

            ok(!balancing, 'already balancing')

            var addresses = Object.keys(lengths)
            if (addresses.length == 0) {
                return step(null, true)
            } else {
                balancer = new Balancer()
                balancing = true
            }

            step(function () {
                step(function (address) {
                    _gather(+address, lengths[address], step())
                })(addresses)
            }, function () {
                tracer('plan', {}, step())
            }, function () {
                var address, node, difference, addresses

                for (address in ordered) {
                    node = ordered[address]
                }

                function terminate (node) {
                    var right
                    if (node) {
                        if (right = node.right) {
                            node.right = null
                            right.left = null
                        }
                    }
                    return right
                }

                function unlink (node) {
                    terminate(node.left)
                    terminate(node)
                    return node
                }

                for (address in lengths) {
                    length = lengths[address]
                    node = ordered[address]
                    difference = node.length - length
                    if (difference > 0 && node.length > options.leafSize) {
                        operations.unshift({
                            method: 'splitLeaf',
                            parameters: [ node.address, node.key, ghosts[node.address] ]
                        })
                        delete ghosts[node.address]
                        unlink(node)
                    }
                }

                for (address in ordered) {
                    if (ordered[address].left) delete ordered[address]
                }

                for (address in ordered) {
                    var node = ordered[address]
                    while (node && node.right) {
                        if (node.length + node.right.length > options.leafSize) {
                            node = terminate(node)
                            ordered[node.address] = node
                        } else {
                            if (node = terminate(node.right)) {
                                ordered[node.address] = node
                            }
                        }
                    }
                }

                for (address in ordered) {
                    node = ordered[address]

                    if (node.right) {
                        ok(!node.right.right, 'merge pair still linked to sibling')
                        operations.unshift({
                            method: 'mergeLeaves',
                            parameters: [ node.right.key, node.key, lengths, !!ghosts[node.address] ]
                        })
                        delete ghosts[node.address]
                        delete ghosts[node.right.address]
                    }
                }

                for (address in ghosts) {
                    node = ghosts[address]
                    if (node.length) operations.unshift({
                        method: 'deleteGhost',
                        parameters: [ node.key ]
                    })
                }

                operate(step())
            })
        })

        var operate = cadence(function (step) {
            step(function () {
                step(function (operation) {
                    methods[operation.method].apply(this, operation.parameters.concat(step()))
                })(operations)
            }, function () {
                balancing = false
                return false
            })
        })

        function shouldSplitBranch (branch, key, callback) {
            if (branch.addresses.length > options.branchSize) {
                if (branch.address == 0) {
                    drainRoot(callback)
                } else {
                    splitBranch(branch.address, key, callback)
                }
            } else {
                callback(null)
            }
        }

        var splitLeaf = cadence(function (step, address, key, ghosts) {
            var locker = new Locker,
                descents = [], replacements = [], encached = [],
                completed = 0,
                penultimate, leaf, split, pages, page,
                records, remainder, right, index, offset, length

            step(function () {
                step([function () {
                    encached.forEach(function (page) { locker.unlock(page) })
                    descents.forEach(function (descent) { locker.unlock(descent.page) })
                    locker.dispose()
                }], function () {
                    if (address != 1 && ghosts) step(function () {
                        deleteGhost(key, step())
                    }, function (rekey) {
                        key = rekey
                    })
                }, function () {
                    descents.push(penultimate = new Descent(locker))

                    penultimate.descend(address == 1 ? penultimate.left : penultimate.key(key),
                                        penultimate.penultimate, step())
                }, function () {
                    penultimate.upgrade(step())
                }, function () {
                    descents.push(leaf = penultimate.fork())
                    leaf.descend(address == 1 ? leaf.left : leaf.key(key), leaf.leaf, step())
                }, function () {
                    split = leaf.page
                    if (split.positions.length - split.ghosts <= options.leafSize) {
                        balancer.unbalanced(split, true)
                        step(null)
                    }
                }, function () {
                    pages = Math.ceil(split.positions.length / options.leafSize)
                    records = Math.floor(split.positions.length / pages)
                    remainder = split.positions.length % pages

                    right = split.right

                    offset = split.positions.length

                    step(function () {
                        page = locker.encache(createLeaf({ loaded: true }))
                        encached.push(page)

                        page.right = right
                        right = page.address

                        splice('addresses', penultimate.page, penultimate.index + 1, 0, page.address)

                        length = remainder-- > 0 ? records + 1 : records
                        offset = split.positions.length - length
                        index = offset


                        step(function () {
                            var position = split.positions[index]

                            ok(index < split.positions.length)

                            step(function () {
                                stash(split, index, step())
                            }, function (entry) {
                                uncacheEntry(split, position)
                                splice('positions', page, page.positions.length, 0, position)
                                splice('lengths', page, page.lengths.length, 0, split.lengths[index])
                                encacheEntry(page, position, entry)
                                index++
                            })
                        })(length)
                    }, function () {
                        splice('positions', split, offset, length)
                        splice('lengths', split, offset, length)

                        var entry = page.cache[page.positions[0]]

                        encacheKey(penultimate.page, page.address, entry.key, entry.keySize)

                        replacements.push(page)

                        rewriteLeaf(page, '.replace', step())
                    })(pages - 1)
                }, function () {
                    split.right = right

                    replacements.push(split)

                    rewriteLeaf(split, '.replace', step())
                }, function () {
                    writeBranch(penultimate.page, '.pending', step())
                }, function () {
                    tracer('splitLeafCommit', {}, step())
                }, function () {
                    rename(penultimate.page, '.pending', '.commit', step())
                }, function () {
                    step(function (page) {
                        replace(page, '.replace', step())
                    })(replacements)
                }, function () {
                    replace(penultimate.page, '.commit', step())
                }, function () {
                    balancer.unbalanced(leaf.page, true)
                    balancer.unbalanced(page, true)
                    return [ encached[0].cache[encached[0].positions[0]].key ]
                })
            }, function (partition) {
                shouldSplitBranch(penultimate.page, partition, step())
            })
        })

        classify.call(methods,mergeLeaves)

        var splitBranch = cadence(function (step, address, key) {
            var locker = new Locker,
                descents = [],
                children = [],
                encached = [],
                parent, full, split, pages,
                records, remainder, offset,
                unwritten, pending

            step(function () {
                step([function () {
                    encached.forEach(function (page) { locker.unlock(page) })
                    descents.forEach(function (descent) { locker.unlock(descent.page) })
                    locker.dispose()
                }], function () {
                    descents.push(parent = new Descent(locker))
                    parent.descend(parent.key(key), parent.child(address), step())
                }, function () {
                    parent.upgrade(step())
                }, function () {
                    descents.push(full = parent.fork())
                    full.descend(full.key(key), full.level(full.depth + 1), step())
                }, function () {
                    split = full.page

                    pages = Math.ceil(split.addresses.length / options.branchSize)
                    records = Math.floor(split.addresses.length / pages)
                    remainder = split.addresses.length % pages

                    offset = split.addresses.length

                    for (var i = 0; i < pages - 1; i++ ) {
                        var page = locker.encache(createBranch({}))

                        children.push(page)
                        encached.push(page)

                        var length = remainder-- > 0 ? records + 1 : records
                        var offset = split.addresses.length - length

                        var cut = splice('addresses', split, offset, length)

                        splice('addresses', parent.page, parent.index + 1, 0, page.address)

                        encacheEntry(parent.page, page.address, split.cache[cut[0]])

                        var keys = {}
                        cut.forEach(function (address) {
                            keys[address] = uncacheEntry(split, address)
                        })

                        splice('addresses', page, 0, 0, cut)

                        cut.slice(1).forEach(function (address) {
                            encacheEntry(page, address, keys[address])
                        })
                    }
                }, function () {
                    children.unshift(full.page)
                    step(function (page) {
                        writeBranch(page, '.replace', step())
                    })(children)
                }, function () {
                    writeBranch(parent.page, '.pending', step())
                }, function () {
                    rename(parent.page, '.pending', '.commit', step())
                }, function () {
                    step(function (page) {
                        replace(page, '.replace', step())
                    })(children)
                }, function () {
                    replace(parent.page, '.commit', step())
                })
            }, function () {
                shouldSplitBranch(parent.page, key, step())
            })
        })

        var drainRoot = cadence(function (step) {
            var locker = new Locker,
                keys = {}, children = [], locks = [],
                root, pages, records, remainder

            step(function () {
                step([function () {
                    children.forEach(function (page) { locker.unlock(page) })
                    locks.forEach(function (page) { locker.unlock(root) })
                    locker.dispose()
                }], function () {
                    locker.lock(0, true, step())
                }, function (locked) {
                    locks.push(root = locked)
                    pages = Math.ceil(root.addresses.length / options.branchSize)
                    records = Math.floor(root.addresses.length / pages)
                    remainder = root.addresses.length % pages

                    for (var i = 0; i < pages; i++) {
                        var page = locker.encache(createBranch({}))

                        children.push(page)

                        var length = remainder-- > 0 ? records + 1 : records
                        var offset = root.addresses.length - length

                        var cut = splice('addresses', root, offset, length)

                        cut.slice(offset ? 0 : 1).forEach(function (address) {
                            keys[address] = uncacheEntry(root, address)
                        })

                        splice('addresses', page, 0, 0, cut)

                        cut.slice(1).forEach(function (address) {
                            encacheEntry(page, address, keys[address])
                        })

                        keys[page.address] = keys[cut[0]]
                    }

                    children.reverse()

                    splice('addresses', root, 0, 0, children.map(function (page) { return page.address }))

                    root.addresses.slice(1).forEach(function (address) {
                        encacheEntry(root, address, keys[address])
                    })
                }, function () {
                    step(function (page) {
                        writeBranch(page, '.replace', step())
                    })(children)
                }, function () {
                    writeBranch(root, '.pending', step())
                }, function () {
                    rename(root, '.pending', '.commit', step())
                }, function () {
                    step(function (page) {
                        replace(page, '.replace', step())
                    })(children)
                }, function () {
                    replace(root, '.commit', step())
                })
            }, function () {
                if (root.addresses.length > options.branchSize) drainRoot(step())
            })
        })

        var exorcise = cadence(function (step, pivot, ghostly, corporal) {
            var entry

            ok(ghostly.ghosts, 'no ghosts')
            ok(corporal.positions.length - corporal.ghosts > 0, 'no replacement')

            uncacheEntry(ghostly, splice('positions', ghostly, 0, 1).shift())
            splice('lengths', ghostly, 0, 1)
            ghostly.ghosts = 0

            step(function () {
                entry = journal.leaf.open(filename(ghostly.address), ghostly.position, ghostly)
                entry.ready(step())
            }, function () {
                writePositions(entry, ghostly, step())
            }, function () {
            // todo: close on failure.
                entry.close('entry', step())
            }, function () {
                stash(corporal, corporal.ghosts, step())
            }, function (entry) {
                uncacheEntry(pivot.page, pivot.page.addresses[pivot.index])
                encacheKey(pivot.page, pivot.page.addresses[pivot.index], entry.key, entry.keySize)
                return [ ghostly.key = entry.key ]
            })
        })

        var deleteGhost = cadence(function (step, key) {
            var locker = new Locker,
                descents = [],
                pivot, leaf, fd
            step([function () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
            }], function () {
                descents.push(pivot = new Descent(locker))
                pivot.descend(pivot.key(key), pivot.found([key]), step())
            }, function () {
                pivot.upgrade(step())
            }, function () {
                descents.push(leaf = pivot.fork())

                leaf.descend(leaf.key(key), leaf.leaf, step())
            }, function () {
                exorcise(pivot, leaf.page, leaf.page, step())
            })
        })
        methods.splitLeaf = splitLeaf
        methods.deleteGhost = deleteGhost

        var mergePages = cadence(function (step, key, leftKey, stopper, merger, ghostly) {
            var locker = new Locker,
                descents = [], singles = { left: [], right: [] }, parents = {}, pages = {},
                ancestor, pivot, empties, ghosted, designation

            function createSingleUnlocker (singles) {
                ok(singles != null, 'null singles')
                return function (parent, child) {
                    if (child.addresses.length == 1) {
                        if (singles.length == 0) singles.push(parent)
                        singles.push(child)
                    } else if (singles.length) {
                        singles.forEach(function (page) { locker.unlock(page) })
                        singles.length = 0
                    } else {
                        locker.unlock(parent)
                    }
                }
            }

            var keys = [ key ]
            if (leftKey) keys.push(leftKey)

            step(function () {
                step([function () {
                    descents.forEach(function (descent) { locker.unlock(descent.page) })
                    ! [ 'left', 'right' ].forEach(function (direction) {
                        if (singles[direction].length) {
                            singles[direction].forEach(function (page) { locker.unlock(page) })
                        } else {
                            locker.unlock(parents[direction].page)
                        }
                    })
                    locker.dispose()
                }], function () {
                    descents.push(pivot = new Descent(locker))
                    pivot.descend(pivot.key(key), pivot.found(keys), step())
                }, function () {
                    var found = pivot.page.cache[pivot.page.addresses[pivot.index]].key
                    if (comparator(found, keys[0]) == 0) {
                        pivot.upgrade(step())
                    } else {
                        step(function () { // left above right
                            pivot.upgrade(step())
                        }, function () {
                            ghosted = { page: pivot.page, index: pivot.index }
                            descents.push(pivot = pivot.fork())
                            keys.pop()
                            pivot.descend(pivot.key(key), pivot.found(keys), step())
                        })
                    }
                }, function () {
                    parents.right = pivot.fork()
                    parents.right.unlocker = createSingleUnlocker(singles.right)
                    parents.right.descend(parents.right.key(key), stopper(parents.right), step())
                }, function () {
                    parents.left = pivot.fork()
                    parents.left.index--
                    parents.left.unlocker = createSingleUnlocker(singles.left)
                    parents.left.descend(parents.left.right,
                                         parents.left.level(parents.right.depth),
                                         step())
                }, function () {
                    if (singles.right.length) {
                        ancestor = singles.right[0]
                    } else {
                        ancestor = parents.right.page
                    }

                    if (leftKey && !ghosted) {
                        if (singles.left.length) {
                            ghosted = { page: singles.left[0], index: parents.left.indexes[singles.left[0].address] }
                        } else {
                            ghosted = { page: parents.left.page, index: parents.left.index }
                            ok(parents.left.index == parents.left.indexes[parents.left.page.address], 'TODO: ok to replace the above')
                        }
                    }

                    descents.push(pages.left = parents.left.fork())
                    pages.left.descend(pages.left.left, pages.left.level(parents.left.depth + 1), step())
                }, function () {
                    descents.push(pages.right = parents.right.fork())
                    pages.right.descend(pages.right.left, pages.right.level(parents.right.depth + 1), step())
                }, function () {
                    merger(pages, ghosted, step())
                }, function (dirty) {
                    if (!dirty) step(null)
                }, function () {
                    rename(pages.right.page, '', '.unlink', step())
                }, function () {
                    var index = parents.right.indexes[ancestor.address]

                    designation = ancestor.cache[ancestor.addresses[index]]

                    var address = ancestor.addresses[index]
                    splice('addresses', ancestor, index, 1)

                    if (pivot.page.address != ancestor.address) {
                        ok(!index, 'expected ancestor to be removed from zero index')
                        ok(ancestor.addresses[index], 'expected ancestor to have right sibling')
                        ok(ancestor.cache[ancestor.addresses[index]], 'expected key to be in memory')
                        designation = ancestor.cache[ancestor.addresses[index]]
                        uncacheEntry(ancestor, ancestor.addresses[0])
                        uncacheEntry(pivot.page, pivot.page.addresses[pivot.index])
                        encacheEntry(pivot.page, pivot.page.addresses[pivot.index], designation)
                    } else{
                        ok(index, 'expected ancestor to be non-zero')
                        uncacheEntry(ancestor, address)
                    }

                    writeBranch(ancestor, '.pending', step())
                }, function () {
                    step(function (page) {
                        rename(page, '', '.unlink', step())
                    })(singles.right.slice(1))
                }, function () {
                    rename(ancestor, '.pending', '.commit', step())
                }, function () {
                    step(function (page) {
                        unlink(page, '.unlink', step())
                    })(singles.right.slice(1))
                }, function () {
                    replace(pages.left.page, '.replace', step())
                }, function () {
                    unlink(pages.right.page, '.unlink', step())
                }, function () {
                    replace(ancestor, '.commit', step())
                })
            }, function () {
                if (ancestor.address == 0) {
                    if (ancestor.addresses.length == 1 && !(ancestor.addresses[0] % 2)) {
                        fillRoot(step())
                    }
                } else {
                    chooseBranchesToMerge(designation.key, ancestor.address, step())
                }
            })
        })

        function mergeLeaves (key, leftKey, unbalanced, ghostly, callback) {
            function stopper (descent) { return descent.penultimate }

            var merger = cadence(function (step, leaves, ghosted) {
                ok(leftKey == null ||
                      comparator(leftKey, leaves.left.page.cache[leaves.left.page.positions[0]].key)  == 0,
                      'left key is not as expected')

                var left = (leaves.left.page.positions.length - leaves.left.page.ghosts)
                var right = (leaves.right.page.positions.length - leaves.right.page.ghosts)

                balancer.unbalanced(leaves.left.page, true)

                var index
                if (left + right > options.leafSize) {
                    if (unbalanced[leaves.left.page.address]) {
                        balancer.unbalanced(leaves.left.page, true)
                    }
                    if (unbalanced[leaves.right.page.address]) {
                        balancer.unbalanced(leaves.right.page, true)
                    }
                    step(null, false)
                } else {
                    step(function () {
                        if (ghostly && left + right) {
                            if (left) {
                                exorcise(ghosted, leaves.left.page, leaves.left.page, step())
                            } else {
                                exorcise(ghosted, leaves.left.page, leaves.right.page, step())
                            }
                        }
                    }, function () {
                        leaves.left.page.right = leaves.right.page.right
                        var ghosts = leaves.right.page.ghosts
                        step(function (index) {
                            index += ghosts
                            step(function () {
                                stash(leaves.right.page, index, step())
                            }, function (entry) {
                                var position = leaves.right.page.positions[index]
                                uncacheEntry(leaves.right.page, position)
                                splice('positions', leaves.left.page, leaves.left.page.positions.length, 0, -(position + 1))
                                splice('lengths', leaves.left.page, leaves.left.page.lengths.length, 0, -(position + 1))
                                encacheEntry(leaves.left.page, -(position + 1), entry)
                            })
                        })(leaves.right.page.positions.length - leaves.right.page.ghosts)
                    }, function () {
                        splice('positions', leaves.right.page, 0, leaves.right.page.positions.length)
                        splice('lengths', leaves.right.page, 0, leaves.right.page.lengths.length)

                        rewriteLeaf(leaves.left.page, '.replace', step())
                    }, function () {
                        return [ true ]
                    })
                }
            })

            mergePages(key, leftKey, stopper, merger, ghostly, callback)
        }

        var chooseBranchesToMerge = cadence(function (step, key, address) {
            var locker = new Locker,
                descents = [],
                designator, choice, lesser, greater, center

            var goToPage = cadence(function (step, descent, address, direction) {
                step(function () {
                    descents.push(descent)
                    descent.descend(descent.key(key), descent.address(address), step())
                }, function () {
                    descent.index += direction == 'left' ? 1 : -1
                    descent.descend(descent[direction], descent.level(center.depth), step())
                })
            })

            var choose = step(function () {
                step([function () {
                    descents.forEach(function (descent) { locker.unlock(descent.page) })
                    locker.dispose()
                }], function () {
                    descents.push(center = new Descent(locker))
                    center.descend(center.key(key), center.address(address), step())
                }, function () {
                    if (center.lesser != null) {
                        goToPage(lesser = new Descent(locker), center.lesser, 'right', step())
                    }
                }, function () {
                    if (center.greater != null) {
                        goToPage(greater = new Descent(locker), center.greater, 'left', step())
                    }
                }, function () {
                    if (lesser && lesser.page.addresses.length + center.page.addresses.length <= options.branchSize) {
                        choice = center
                    } else if (greater && greater.page.addresses.length + center.page.addresses.length <= options.branchSize) {
                        choice = greater
                    }

                    if (choice) {
                        descents.push(designator = choice.fork())
                        designator.descend(designator.left, designator.leaf, step())
                    } else {
                        // todo: return [ choose ] does not invoke finalizer.
                        // return [ choose ]
                        step(null)
                    }
                }, function () {
                    stash(designator.page, 0, step())
                })
            }, function (entry) {
                if (entry) { // todo: fix return [ choose ]
                    mergeBranches(entry.key, entry.keySize, choice.page.address, step())
                }
            })(1)
        })

        function mergeBranches (key, keySize, address, callback) {
            function stopper (descent) {
                return descent.child(address)
            }

            var merger = cadence(function (step, pages, ghosted) {
                ok(address == pages.right.page.address, 'unexpected address')

                var cut = splice('addresses', pages.right.page, 0, pages.right.page.addresses.length)

                var keys = {}
                cut.slice(1).forEach(function (address) {
                    keys[address] = uncacheEntry(pages.right.page, address)
                })

                splice('addresses', pages.left.page, pages.left.page.addresses.length, 0, cut)
                cut.slice(1).forEach(function (address) {
                    encacheEntry(pages.left.page, address, keys[address])
                })
                ok(cut.length, 'cut is zero length')
                encacheKey(pages.left.page, cut[0], key, keySize)

                step(function () {
                    writeBranch(pages.left.page, '.replace', step())
                }, function () {
                    return [ true ]
                })
            })

            mergePages(key, null, stopper, merger, false, callback)
        }

        var fillRoot = cadence(function (step) {
            var locker = new Locker, descents = [], root, child

            step([function () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
            }], function () {
                descents.push(root = new Descent(locker))
                root.exclude()
                root.descend(root.left, root.level(0), step())
            }, function () {
                descents.push(child = root.fork())
                child.descend(child.left, child.level(1), step())
            }, function () {
                var cut
                ok(root.page.addresses.length == 1, 'only one address expected')
                ok(!Object.keys(root.page.cache).length, 'no keys expected')

                splice('addresses', root.page, 0, root.page.addresses.length)

                cut = splice('addresses', child.page, 0, child.page.addresses.length)

                var keys = {}
                cut.slice(1).forEach(function (address) {
                    keys[address] = uncacheEntry(child.page, address)
                })

                splice('addresses', root.page, root.page.addresses.length, 0, cut)
                cut.slice(1).forEach(function  (address) {
                    encacheEntry(root.page, address, keys[address])
                })

                writeBranch(root.page, '.pending', step())
            }, function () {
                rename(child.page, '', '.unlink', step())
            }, function () {
                rename(root.page, '.pending', '.commit', step())
            }, function () {
                unlink(child.page, '.unlink', step())
            }, function () {
                replace(root.page, '.commit', step())
            })
        })

        this.balance = balance
        return classify.call(this, unbalanced)
    }

    function left (descents, exclusive, callback) {
        toLeaf(descents[0].left, descents, null, exclusive, callback)
    }

    function right (descents, exclusive, callback) {
        toLeaf(descents[0].right, descents, null, exclusive, callback)
    }

    function key(key) {
        return function (descents, exclusive, callback) {
            toLeaf(descents[0].key(key), descents, null, exclusive, callback)
        }
    }

    function leftOf (key) {
        return cadence(function (step, descents, exclusive) {
            // todo: outgoing
            thrownByUser = null
            var conditions = [ descents[0].leaf, descents[0].found([key]) ]
            step(function () {
                descents[0].descend(descents[0].key(key), function () {
                    return conditions.some(function (condition) {
                        return condition()
                    })
                }, step())
            }, function (page, index) {
                if (descents[0].page.address % 2) {
                    return [ new Cursor(createJournal(), descents, false, key) ]
                } else {
                    descents[0].index--
                    toLeaf(descents[0].right, descents, null, exclusive, step())
                }
            })
        })
    }

    var toLeaf = cadence(function (step, sought, descents, key, exclusive) {
        thrownByUser = null
        step(function () {
            descents[0].descend(sought, descents[0].penultimate, step())
        }, function () {
            if (exclusive) descents[0].exclude()
            descents[0].descend(sought, descents[0].leaf, step())
        }, function () {
            return [ new Cursor(createJournal(), descents, exclusive, key) ]
        })
    })

    // to user land
    var cursor = cadence(function (step, key, exclusive) {
        var descents = [ new Descent(new Locker) ]
        step([function () {
            if (descents.length) {
                descents[0].locker.unlock(descents[0].page)
                descents[0].locker.dispose()
            }
        }], function () {
            if  (typeof key == 'function') {
                key(descents, exclusive, step())
            } else {
                toLeaf(descents[0].key(key), descents, key, exclusive, step())
            }
        }, function (cursor) {
            return [ cursor ]
        })
    })

    function iterator (key, callback) {
        cursor(key, false, callback)
    }

    function mutator (key, callback) {
        cursor(key, true, callback)
    }

    // to user land
    function balance (callback) {
        balancer.balance(callback)
    }

    // to user land
    var vivify = cadence(function (step) {
        var locker = new Locker, root

        function record (address) {
            return { address: address }
        }

        step(function () {
            locker.lock(0, false, step())
        }, function (page) {
            step([function () {
                locker.unlock(page)
                locker.dispose()
            }], function () {
                expand(page, root = page.addresses.map(record), 0, step())
            })
        })

        var expand = cadence(function (step, parent, pages, index) {
            var block = step(function () {
                if (index < pages.length) {
                    var address = pages[index].address
                    locker.lock(address, false, step(step, [function (page) { locker.unlock(page) }]))
                } else {
                    return [ block, pages ]
                }
            }, function (page) {
                if (page.address % 2 == 0) {
                    step(function () {
                        pages[index].children = page.addresses.map(record)
                        if (index) {
                            pages[index].key = parent.cache[parent.addresses[index]].key
                        }
                        expand(page, pages[index].children, 0, step())
                    }, function () {
                        expand(parent, pages, index + 1, step())
                    })
                } else {
                    step(function () {
                        pages[index].children = []
                        pages[index].ghosts = page.ghosts

                        step(function (recordIndex) {
                            step(function () {
                                stash(page, recordIndex, step())
                            }, function (entry) {
                                pages[index].children.push(entry.record)
                            })
                        })(page.positions.length)
                    }, function () {
                        expand(parent, pages, index + 1, step())
                    })
                }
            })(1)
        })
    })

    function purge (downTo) {
        var purge = magazine.purge()
        while (purge.cartridge && magazine.heft > downTo) {
            purge.cartridge.remove()
            purge.next()
        }
        purge.release()
    }

    var objectToReturn = classify.call(this, create, open,
                               key, left, leftOf, right,
                               iterator, mutator,
                               balance, purge, vivify,
                               close,
                               _size, _nextAddress)
    this.create = create
    this.open = open
    this.close = close
    this.vivify = vivify
    return objectToReturn
}

module.exports = Strata
