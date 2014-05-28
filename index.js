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
    var sequester = options.sequester || require('sequester'),
        directory = options.directory,
        extractor = options.extractor || extract,
        comparator = options.comparator || compare,
        fs = options.fs || require('fs'),
        path = options.path || require('path'),
        ok = function (condition, message) { if (!condition) throw new Error(message) },
        cache = options.cache || (new (require('magazine'))),
        thrownByUser,
        magazine,
        nextAddress = 0,
        length = 1024,
        balancer = new Balancer(),
        balancing,
        size = 0,
        checksum,
        constructors = {},
        serialize = options.serialize || function (object) { return new Buffer(JSON.stringify(object)) },
        deserialize = options.deserialize || function (buffer) { return JSON.parse(buffer.toString()) },
        tracer = options.tracer || function () { arguments[2]() }

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

    function validator (callback, janitor) {
        return function (forward, janitor2) { return validate(callback, forward, janitor2 || janitor) }
    }

    function validate (callback, forward, janitor) {
        ok(typeof forward == 'function', 'no forward function')
        ok(typeof callback == 'function','no callback function')

        return function (error) {
            if (error) {
                cleanup(error)
            } else {
                try {
                    forward.apply(null, __slice.call(arguments, 1))
                } catch (error) {
                    cleanup(error)
                }
            }
        }

        function cleanup (error) {
            if (janitor) {
                janitor(error)
                if (janitor.length) return
            }

            if (thrownByUser === error) {
                throw error
            }

            toUserLand(callback, error)
        }
    }

    function toUserLand (callback) {
        try {
            callback.apply(null, __slice.call(arguments, 1))
        } catch (error) {
            thrownByUser = error
            throw error
        }
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

    function replace (page, suffix, callback) {
        var replacement = filename(page.address, suffix),
            permanent = filename(page.address)

        fs.stat(replacement, validator(callback)(stat))

        function stat (stat) {
            ok(stat.isFile(), 'is not a file')
            fs.unlink(permanent, unlinked)
        }

        function unlinked (error) {
            if (error && error.code != 'ENOENT') callback(error)
            else fs.rename(replacement, permanent, callback)
        }
    }

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

    var cadence = require('cadence')

    function Transcript (file, flags, position) {
        this._file = file
        this._position = position
        this._stream = fs.createWriteStream(this._file, {
            flags: flags,
            mode: 0644,
            start: this._position
        }).once('open', function (error) {
            this._opened = true
        }.bind(this))
    }

    Transcript.prototype.ready = cadence(function (step) {
        step(function () {
            if (!this._opened) {
                this._stream.once('open', step(-1))
                this._stream.once('error', step(Error))
            }
        }, function () {
            this._stream.removeAllListeners('error')
            this._stream.on('error', function (error) {
                this._error = error
            }.bind(this))
        })
    })

    Transcript.prototype._checkError = function () {
        if (this._error) {
            var error = this._error
            this._error = new Error('already errored')
            throw error
        }
    }

    Transcript.prototype.write = cadence(function (step, buffer) {
        this._checkError()
        if (!this._stream.write(buffer)) { // <- does this 'error' if `true`?
            step(function () {
                this._stream.once('drain', step(-1))
                this._stream.once('error', step(Error))
            }, function () {
                this._stream.removeAllListeners('error')
            })
        }
    })

    Transcript.prototype.close = cadence(function (step) {
        step(function () {
            this._checkError() // <- would `error` be here?
            this._stream.removeAllListeners('error')
            this._stream.end(step())
            this._stream.once('error', step(Error)) // <- will this called?
        }, function () {
            this._stream.removeAllListeners('error')
            this._error = new Error('closed')
        })
    })

    function cookEntry (options, author) {
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

        author(buffer, body, options.page.position, length)
    }

    function writeEntry4 (options, callback) {
        cookEntry(options, function (buffer, body, position, length) {
            var check = validator(callback), transcript = options.transcript

            transcript.write(buffer, check(sent))

            function sent () {
                options.page.position += length
                callback(null, position, length, body && body.length)
            }
        })
    }

    function writeEntry (options, callback) {
        cookEntry(options, function (buffer, body, position,length) {
            var check = validator(callback),
                offset = 0

            send()

            function send () {
                fs.write(options.fd, buffer, offset, buffer.length - offset, options.page.position, check(sent))
            }

            function sent(written) {
                options.page.position += written
                offset += written
                if (offset == buffer.length) {
                    if (!(options.page.address % 2) || options.type == 'footer') {
                        callback(null, position, length)
                    } else {
                        writeFooter(options.fd, options.page, function () {
                            callback(null, position, length, body && body.length)
                        })
                    }
                } else {
                    send()
                }
            }
        })
    }

    function writeEntry3 (options, callback) {
        cookEntry(options, function (buffer, body, position,length) {
            var check = validator(callback),
                offset = 0

            send()

            function send () {
                fs.write(options.fd, buffer, offset, buffer.length - offset, options.page.position, check(sent))
            }

            function sent(written) {
                options.page.position += written
                offset += written
                if (offset == buffer.length) {
                    callback(null, position, length, body && body.length)
                } else {
                    send()
                }
            }
        })
    }

    function writeInsert3 (transcript, page, index, record, callback) {
        var header = [ ++page.entries, index + 1 ]
        writeEntry4({ transcript: transcript, page: page, header: header, body: record }, callback)
    }

    function writeInsert2 (fd, page, index, record, callback) {
        var header = [ ++page.entries, index + 1 ]
        writeEntry3({ fd: fd, page: page, header: header, body: record }, callback)
    }

    function writeDelete (transcript, page, index, callback) {
        var header = [ ++page.entries, -(index + 1) ]
        writeEntry4({ transcript: transcript, page: page, header: header }, callback)
    }

    function io (direction, filename, callback) {
        var check = validator(callback)

        fs.open(filename, direction[0], check(opened))

        function opened (fd) {
            fs.fstat(fd, check(stat))

            function stat (stat) {
                callback(null, fd, stat, function (buffer, position, callback) {
                    var check = validator(callback), offset = 0

                    var length = stat.size - position
                    var slice = length < buffer.length ? buffer.slice(0, length) : buffer

                    done(0)

                    function done (count) {
                        if (count < slice.length - offset) {
                            offset += count
                            fs[direction](fd, slice, offset, slice.length - offset, position + offset, check(done))
                        } else {
                            callback(null, slice, position)
                        }
                    }
                })
            }
        }
    }

    function writePositions (fd, page, callback) {
        var header = [ ++page.entries, 0, page.ghosts ]
        header = header.concat(page.positions).concat(page.lengths)
        writeEntry3({ fd: fd, page: page, header: header, type: 'position' }, callback)
    }

    function writeFooter (fd, page, callback) {
        ok(page.address % 2 && page.bookmark != null)
        var header = [
            0, page.bookmark.position, page.bookmark.length, page.bookmark.entry,
            page.right || 0, page.position, page.entries, page.ghosts, page.positions.length - page.ghosts
        ]
        writeEntry({ fd: fd, page: page, header: header, type: 'footer' }, validate(callback, function (position, length) {
            page.position = header[5]
            callback(null, position, length)
        }))
    }

    function writeFooter2 (transcript, page, callback) {
        ok(page.address % 2 && page.bookmark != null)
        var header = [
            0, page.bookmark.position, page.bookmark.length, page.bookmark.entry,
            page.right || 0, page.position, page.entries, page.ghosts, page.positions.length - page.ghosts
        ]
        cookEntry({
            page: page,
            header: header,
            type: 'footer'
        }, function (buffer, body, position, length) {
            var check = validator(callback)
            transcript.write(buffer, check(sent))
            function sent () {
//                page.position = header[5]
                callback(null, position, length)
            }
        })
    }

    function writeFooter3 (fd, page, callback) {
        ok(page.address % 2 && page.bookmark != null)
        var header = [
            0, page.bookmark.position, page.bookmark.length, page.bookmark.entry,
            page.right || 0, page.position, page.entries, page.ghosts, page.positions.length - page.ghosts
        ]
        writeEntry3({ fd: fd, page: page, header: header, type: 'footer' }, validate(callback, function (position, length) {
            page.position = header[5]
            callback(null, position, length)
        }))
    }

    function writeFooter4 (transcript, page, callback) {
        ok(page.address % 2 && page.bookmark != null)
        var header = [
            0, page.bookmark.position, page.bookmark.length, page.bookmark.entry,
            page.right || 0, page.position, page.entries, page.ghosts, page.positions.length - page.ghosts
        ]
        writeEntry4({
            transcript: transcript,
            page: page,
            header: header,
            type: 'footer'
        }, validate(callback, function (position, length) {
            page.position = header[5] // todo: can't we use `position`?
            callback(null, position, length)
        }))
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

    function readLeaf (page, callback) {
        var positions = [],
            lengths = [],
            bookmark,
            check = validator(callback)

        if (options.replay) {
            io('read', filename(page.address), check(forward))
        } else {
            io('read', filename(page.address), check(backward))
        }

        function forward (fd, stat, read) {
            page.entries = 0
            page.ghosts = 0
            replay(fd, stat, read, page, 0, check(done))
        }

        function backward (fd, stat, read) {
            var buffer = new Buffer(options.readLeafStartLength || 1024)
            read(buffer, Math.max(0, stat.size - buffer.length), check(footer))

            function footer (slice) {
                for (var i = slice.length - 2; i != -1; i--) {
                    if (slice[i] == 0x0a) {
                        var footer = readFooter(readEntry(slice.slice(i + 1)))
                        ok(!footer.entry, 'footer is supposed to be zero')
                        bookmark = footer.bookmark
                        page.right = footer.right
                        page.position = footer.position
                        ok(page.position != null, 'no page position')
                        read(new Buffer(bookmark.length), bookmark.position, check(positioned))
                        return
                    }
                }
                throw new Error('cannot find footer in last ' + buffer.length + ' bytes')
            }

            function positioned (slice) {
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

                replay(fd, stat, read, page, bookmark.position + bookmark.length, check(done))
            }
        }

        function done () {
            callback(null, page)
        }
    }

    function replay (fd, stat, read, page, position, callback) {
        var check = validator(callback),
            leaf = !!(page.address % 2),
            seen = {},
            buffer = new Buffer(options.readLeafStartLength || 1024),
            footer

        read(buffer, position, check(replay, close))

        function replay (slice, start) {
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
                    read(buffer, start, check(replay, close))
                } else {
                    read(buffer, start + offset, check(replay, close))
                }
            } else {
                close()
            }
        }

        function close (replayError) {
            fs.close(fd, check(closed, closed))

            function closed (closeError) {
                callback(replayError || closeError, page, footer)
            }
        }
    }

    function readRecord (page, position, length, callback) {
        var check = validator(callback), entry

        io('read', filename(page.address), check(input))

        function input (fd, stat, read) {
            tracer('readRecord', { page: page }, check(record))

            function record () {
                read(new Buffer(length), position, check(json, close))
            }

            function json (buffer) {
                ok(buffer[length - 1] == 0x0A, 'newline expected')
                entry = readEntry(buffer, false)
                close()
            }

            function close (readError) {
                fs.close(fd, check(closed))

                function closed (closeError) {
                    callback(readError || closeError, entry)
                }
            }
        }
    }

    function rewriteLeaf (page, suffix, callback) {
        var check = validator(callback),
            cache = {},
            index = 0,
            fd, positions, lengths

        fs.open(filename(page.address, suffix), 'w', 0644, check(opened))

        function opened ($1) {
            fd = $1

            page.position = 0
            page.entries = 0

            positions = splice('positions', page, 0, page.positions.length)
            lengths = splice('lengths', page, 0, page.lengths.length)

            writePositions(fd, page, check(iterate))
        }

        function iterate () {
            if (positions.length) rewrite()
            else if (page.positions.length) append()
            else footer()
        }

        function rewrite () {
            var position = positions.shift(), length = lengths.shift(), entry

            stash(page, position, length, check(stashed))

            function stashed ($) {
                uncacheEntry(page, position)
                writeInsert2(fd, page, index++, (entry = $).record, check(written))
            }

            function written (position, length) {
                splice('positions', page, page.positions.length, 0, position)
                splice('lengths', page, page.lengths.length, 0, length)
                cache[position] = entry
                iterate()
            }
        }

        function append() {
            var entry
            for (var position in cache) {
                entry = cache[position]
                encacheEntry(page, position, entry)
            }
            writePositions(fd, page, check(footer))
        }

        function footer () {
            writeFooter3(fd, page, check(close))
        }

        function close() {
            fs.close(fd, callback)
        }
    }

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

    function writeBranch (page, suffix, callback) {
        var check = validator(callback),
            addresses = page.addresses.slice(),
            keys = addresses.map(function (address, index) { return page.cache[address] }),
            transcript

        ok(keys[0] === (void(0)), 'first key is null')
        ok(keys.slice(1).every(function (key) { return key != null }), 'null keys')

        page.entries = 0
        page.position = 0

        transcript = new Transcript(filename(page.address, suffix), 'w', 0)

        transcript.ready(check(ready))

        function ready () {
            write()
        }

        function write () {
            if (addresses.length) {
                var address = addresses.shift()
                var key = page.entries ? page.cache[address].key : null
                page.entries++
                var header = [ page.entries, page.entries, address ]
                writeEntry4({ transcript: transcript, page: page, header: header, body: key, isKey: true }, check(write))
            } else {
                transcript.close(check(closed))
            }
        }

        function closed () {
            callback(null)
        }
    }

    function readBranch (page, callback) {
        var check = validator(callback)
        io('read', filename(page.address), check(opened))

        function opened (fd, stat, read) {
            replay(fd, stat, read, page, 0, callback)
        }
    }

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

    function create (callback) {
        var check = validator(callback), locker = new Locker, count = 0, root, leaf

        magazine = createMagazine()

        fs.stat(directory, check(extant))

        function extant (stat) {
            ok(stat.isDirectory(), 'database ' + directory + ' is not a directory.')
            fs.readdir(directory, check(empty))
        }

        function empty (files) {
            ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
                  'database ' + directory + ' is not empty.')

            root = locker.encache(createBranch({ penultimate: true }))
            leaf = locker.encache(createLeaf({}))
            splice('addresses', root, 0, 0, leaf.address)

            writeBranch(root, '.replace', check(branchWritten))
        }

        function branchWritten () {
            rewriteLeaf(leaf, '.replace', check(leafWritten))
        }

        function leafWritten () {
            replace(root, '.replace', check(branchReplaced))
        }

        function branchReplaced () {
            replace(leaf, '.replace', check(leafReplaced))
        }

        function leafReplaced() {
            locker.unlock(root)
            locker.unlock(leaf)
            locker.dispose()
            toUserLand(callback)
        }
    }

    function open (callback) {
        var check = validator(callback)

        magazine = createMagazine()

        fs.stat(directory, check(stat))

        function stat (error, stat) {
            fs.readdir(directory, check(list))
        }

        function list (files) {
            files.forEach(function (file) {
                if (/^\d+$/.test(file)) {
                    nextAddress = Math.max(+(file) + 1, nextAddress)
                }
            })
            toUserLand(callback, null)
        }
    }

    function close (callback) {
        var cartridge = magazine.get(-2), lock = cartridge.value.page.lock

        lock.unlock()
        // todo
        lock.dispose()

        cartridge.release()

        magazine.purge(-2)

        ok(!magazine.count, 'pages still held by cache')

        thrownByUser = null

        toUserLand(callback, null)
    }

    function stash (page, positionOrIndex, length, callback) {
        var position = positionOrIndex
        if (arguments.length == 3) {
            callback = length
            position = page.positions[positionOrIndex]
            length = page.lengths[positionOrIndex]
        }
        ok(length)
        var entry, loader
        if (loader = page.loaders[position]) {
            loader.share(callback)
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
            stash(page, position, length, callback)
        } else {
            callback(null, entry, length)
        }
    }

    function unwind (callback) {
        var vargs = __slice.call(arguments, 1)
        if (options.setImmediate) setImmediate(function () { callback.apply(null, vargs) })
        else callback.apply(null, vargs)
    }

    function _find (page, key, low, callback) {
        var mid, high = (page.addresses || page.positions).length - 1, check = validator(callback)

        if (page.address % 2) test()
        else callback(null, find())

        function find () {
            while (low <= high) {
                mid = low + ((high - low) >>> 1)
                var compare = comparator(key, page.cache[page.addresses[mid]].key)
                if (compare < 0) high = mid - 1
                else if (compare > 0) low = mid + 1
                else return mid
            }
            return ~low
        }

        function test () {
            if (low <= high) {
                mid = low + ((high - low) >>> 1)
                stash(page, mid, check(function (entry) { compare(entry.key) }))
            } else {
                unwind(callback, null, ~low)
            }
        }

        function compare (other) {
            ok(other != null, 'key is null in find')
            var compare = comparator(key, other)
            if (compare == 0) {
                unwind(callback, null, mid)
            } else {
                if (compare > 0) low = mid + 1
                else high = mid - 1
                test()
            }
        }
    }

    function Locker () {
        var locks ={}

        function lock (address, exclusive, callback) {
            var check = validator(callback, release),
                cartridge = magazine.hold(address, {}), page = cartridge.value.page, locked

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

            locks[page.address][exclusive ? 'exclude' : 'share'](check(trace))

            function trace () {
                tracer('lock', { address: address, exclusive: exclusive }, check(complete))
            }

            function complete () {
                locked = true
                callback(null, page)
            }

            function release (error) {
                if (!locked) {
                    magazine.get(page.address).release()
                    locks[page.address].unlock(error)
                    delete locks[page.address]
                }
                callback(error)
            }
        }

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

        return classify.call(this, lock, encache, increment, unlock, dispose)
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

        function upgrade (callback) {
            locker.unlock(page)

            locker.lock(page.address, exclusive = true, validate(callback, locked, failed))

            function locked (locked) {
                page = locked
                callback(null)
            }

            function failed () {
                locker.lock(-2, false, function (error, locked) {
                    ok(!error, 'impossible error')
                    page = locked
                })
                ok(page, 'dummy page not in cache')
            }
        }

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

        function descend (next, stop, callback) {
            var check = validator(callback), above = page

            downward()

            function downward () {
                if (stop()) {
                    unwind(callback, null, page, index)
                } else {
                    if (index + 1 < page.addresses.length) {
                        greater = page.address
                    }
                    if (index > 0) {
                        lesser = page.address
                    }
                    locker.lock(page.addresses[index], exclusive, check(locked))
                }
            }

            function locked (locked) {
                depth++
                unlocker(page, locked)
                page = locked
                next(check(directed))
            }

            function directed ($index) {
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
                downward()
            }
        }

        return classify.call(this, descend, fork, exclude, upgrade,
                                   key, left, right,
                                   found, address, child, penultimate, leaf, level,
                                   _locker, _page, _depth, _index, index_, _indexes, _lesser, _greater,
                                   unlocker_)
    }

    function Cursor (descents, exclusive, searchKey) {
        var locker = descents[0].locker,
            page = descents[0].page,
            rightLeafKey = null,
            length = page.positions.length,
            index = descents[0].index,
            offset = index < 0 ? ~ index : index

        descents.shift()

        function get (index, callback) {
            stash(page, index, validate(callback, function (entry, size) {
                toUserLand(callback, null, entry.record, entry.key, size)
            }))
        }

        function next (callback) {
            var next

            rightLeafKey = null

            if (page.right) {
                locker.lock(page.right, exclusive, validate(callback, locked))
            } else {
                toUserLand(callback, null, false)
            }

            function locked (next) {
                locker.unlock(page)

                page = next

                offset = page.ghosts
                length = page.positions.length

                toUserLand(callback, null, true)
            }
        }

        function indexOf (key, callback) {
            _find(page, key, page.ghosts, callback)
        }

        function unlock () {
            locker.unlock(page)
            locker.dispose()
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

        if (!exclusive) return this

        function insert (record, key, index, callback) {
            var check = validator(callback), unambiguous

            if (index == 0 && page.address != 1) {
                toUserLand(callback, null, -1)
                return
            }

            unambiguous = index < page.positions.length
            unambiguous = unambiguous || searchKey.length && comparator(searchKey[0], key) == 0
            unambiguous = unambiguous || ! page.right

            if (unambiguous) insert()
            else ambiguity()

            function ambiguity () {
                if (rightLeafKey) {
                    compare()
                } else {
                    locker.lock(page.right, false, check(load))
                }

                function load (rightLeafPage) {
                    stash(rightLeafPage, 0, validate(callback, designated, release))

                    function designated (entry) {
                        rightLeafKey = entry.key
                        release()
                        compare()
                    }

                    function release () {
                        locker.unlock(rightLeafPage)
                    }
                }

                function compare () {
                    if (comparator(key, rightLeafKey) < 0) insert()
                    else toUserLand(callback, null, +1)
                }
            }

            function insert () {
                var transcript

                balancer.unbalanced(page)

                transcript = new Transcript(filename(page.address), 'r+', page.position)

                transcript.ready(check(ready))

                function ready () {
                    writeInsert3(transcript, page, index, record, check(inserted, close))
                }

                function inserted (position, length, size) {
                    writeFooter4(transcript, page, check(written)) // todo: else close

                    function written () {
                        splice('positions', page, index, 0, position)
                        splice('lengths', page, index, 0, length)
                        _cacheRecord(page, position, record, size)

                        length = page.positions.length

                        close()
                    }
                }

                function close (writeError) {
                    transcript.close(validate(callback, complete, complete))

                    function complete (closeError) {
                        toUserLand(callback, writeError || closeError, 0)
                    }
                }
            }
        }

        function remove (index, callback) {
            var ghost = page.address != 1 && index == 0,
                check = validator(callback),
                transcript

            balancer.unbalanced(page)

            transcript = new Transcript(filename(page.address), 'r+', page.position)

            transcript.ready(check(ready))

            function ready () {
                writeDelete(transcript, page, index, check(deleted, close))
            }

            function deleted () {
                writeFooter4(transcript, page, check(written, close))
            }

            function written () {
                if (ghost) {
                    page.ghosts++
                    offset || offset++
                } else {
                    uncacheEntry(page, page.positions[index])
                    splice('positions', page, index, 1)
                    splice('lengths', page, index, 1)
                }
                close()
            }

            function close (writeError) {
                transcript.close(check(complete, complete))

                function complete (closeError) {
                    toUserLand(callback, writeError || closeError)
                }
            }
        }

        return classify.call(this, insert, remove)
    }

    function Balancer () {
        var lengths = {},
            operations = [],
            referenced = {},
            ordered = {},
            ghosts = {},
            methods = {}

        classify.call(methods, deleteGhost, splitLeaf, mergeLeaves)

        function unbalanced (page, force) {
            if (force) {
                lengths[page.address] = options.leafSize
            } else if (lengths[page.address] == null) {
                lengths[page.address] = page.positions.length - page.ghosts
            }
        }

        function balance (callback) {
            var check = validator(callback), locker = new Locker, address

            ok(!balancing, 'already balancing')

            var addresses = Object.keys(lengths)
            if (addresses.length == 0) {
                callback(null, true)
            } else {
                balancer = new Balancer()
                balancing = true
                gather()
            }

            function gather () {
                var address = +(addresses.shift()), length = lengths[address], right, node

                if (node = ordered[address]) checkMerge(node)
                else locker.lock(address, false, nodify(checkMerge))

                function nodify (next) {
                    return check(function (page) {
                        ok(page.address % 2, 'leaf page expected')

                        if (page.address == 1) identified({})
                        else stash(page, 0, validate(callback, identified, unlock))

                        function identified (entry) {
                            node = {
                                key: entry.key,
                                address: page.address,
                                rightAddress: page.right,
                                length: page.positions.length - page.ghosts
                            }
                            unlock()
                            ordered[node.address] = node
                            if (page.ghosts)
                                ghosts[node.address] = node
                            tracer('reference', {}, check(traced))
                        }

                        function traced () { next(node) }

                        function unlock () { locker.unlock(page) }
                    })
                }

                function checkMerge(node) {
                    if (node.length - length < 0) {
                        if (node.address != 1 && ! node.left) leftSibling(node)
                        else rightSibling(node)
                    } else {
                        next()
                    }
                }

                function leftSibling (node) {
                    var descent = new Descent(locker)
                    descent.descend(descent.key(node.key), descent.found([node.key]), check(goToLeaf))

                    function goToLeaf () {
                        descent.index--
                        descent.descend(descent.right, descent.leaf, validate(callback, checkLists, unlock))
                    }

                    function checkLists () {
                        var left
                        if (left = ordered[descent.page.address]) {
                            unlock()
                            attach(left)
                        } else {
                            nodify(attach)(null, descent.page)
                        }
                    }

                    function attach (left) {
                        left.right = node
                        node.left = left

                        rightSibling(node)
                    }

                    function unlock () {
                       locker.unlock(descent.page)
                    }
                }

                function rightSibling (node) {
                    var right

                    if (!node.right && node.rightAddress)  {
                        if (right = ordered[node.rightAddress]) attach(right)
                        else locker.lock(node.rightAddress, false, nodify(attach))
                    } else {
                        next()
                    }

                    function attach (right) {
                        node.right = right
                        right.left = node

                        next()
                    }
                }

                function next () {
                    if (addresses.length) {
                        gather()
                    } else {
                        locker.dispose()
                        tracer('plan', {}, check(traced))
                    }
                }

                function traced () { plan(callback) }
            }
        }

        function plan (callback) {
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
                    operations.unshift({  method: 'splitLeaf', parameters: [ node.key, ghosts[node.address] ] })
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

            operate(callback)
        }

        function operate (callback) {
            var check = validator(callback), address
            function shift () {
                var operation = operations.shift()
                if (operation) {
                    methods[operation.method].apply(this, operation.parameters.concat(check(shift)))
                } else {
                    balancing = false
                    callback(null, false)
                }
            }
            shift()
        }

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

        function splitLeaf (key, ghosts, callback) {
            var locker = new Locker,
                check = validator(callback, release),
                descents = [], replacements = [], encached = [],
                completed = 0,
                penultimate, leaf, split, pages, page,
                records, remainder, right, index, offset, length

            if (ghosts) deleteGhost(key, check(exorcised))
            else penultimate()

            function exorcised (rekey) {
                key = rekey
                penultimate()
            }

            function penultimate () {
                descents.push(penultimate = new Descent(locker))

                penultimate.descend(penultimate.key(key), penultimate.penultimate, check(upgrade))
            }

            function upgrade () {
                penultimate.upgrade(check(fork))
            }

            function fork () {
                descents.push(leaf = penultimate.fork())
                leaf.descend(leaf.key(key), leaf.leaf, check(dirty))
            }

            function dirty () {
                split = leaf.page

                if (split.positions.length - split.ghosts <= options.leafSize) {
                    balancer.unbalanced(split, true)
                    release()
                    callback(null)
                } else {
                    partition()
                }
            }

            function partition () {
                pages = Math.ceil(split.positions.length / options.leafSize)
                records = Math.floor(split.positions.length / pages)
                remainder = split.positions.length % pages

                right = split.right

                offset = split.positions.length

                paginate()
            }

            function paginate () {
                if (--pages) shuffle()
                else paginated()
            }

            function shuffle () {
                page = locker.encache(createLeaf({ loaded: true }))
                encached.push(page)

                page.right = right
                right = page.address

                splice('addresses', penultimate.page, penultimate.index + 1, 0, page.address)

                length = remainder-- > 0 ? records + 1 : records
                offset = split.positions.length - length
                index = offset

                copy()
            }

            function copy () {
                var position = split.positions[index]

                ok(index < split.positions.length)

                stash(split, index, check(uncache))

                function uncache (entry) {
                    uncacheEntry(split, position)
                    splice('positions', page, page.positions.length, 0, position)
                    splice('lengths', page, page.lengths.length, 0, split.lengths[index])
                    encacheEntry(page, position, entry)
                    index++
                    if (index < offset + length) copy()
                    else copied()
                }
            }

            function copied() {
                splice('positions', split, offset, length)
                splice('lengths', split, offset, length)

                var entry = page.cache[page.positions[0]]

                encacheKey(penultimate.page, page.address, entry.key, entry.keySize)

                replacements.push(page)

                rewriteLeaf(page, '.replace', check(replaced))
            }

            function replaced () {
                paginate()
            }

            function paginated () {
                split.right = right

                rewriteLeaf(split, '.replace', check(transact))

                replacements.push(split)
            }

            function transact () {
                writeBranch(penultimate.page, '.pending', check(trace))
            }

            function trace () {
                tracer('splitLeafCommit', {}, check(commit))
            }

            function commit () {
                rename(penultimate.page, '.pending', '.commit', check(persist))
            }

            function persist () {
                if (replacements.length) replace(replacements.shift(), '.replace', check(persist))
                else complete()
            }

            function complete () {
                replace(penultimate.page, '.commit', check(rebalance))
            }

            function rebalance () {
                balancer.unbalanced(leaf.page, true)
                balancer.unbalanced(page, true)

                release()

                shouldSplitBranch(penultimate.page, key, callback)
            }

            function release () {
                encached.forEach(function (page) { locker.unlock(page) })
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()

            }
        }

        function splitBranch (address, key, callback) {
            var check = validator(callback, release),
                locker = new Locker,
                descents = [],
                children = [],
                encached = [],
                parent, full, split, pages,
                records, remainder, offset

            descents.push(parent = new Descent(locker))

            parent.descend(parent.key(key), parent.child(address), check(upgrade))

            function upgrade () {
                parent.upgrade(check(fork))
            }

            function fork () {
                descents.push(full = parent.fork())
                full.descend(full.key(key), full.level(full.depth + 1), check(partition))
            }

            function partition () {
                split = full.page

                pages = Math.ceil(split.addresses.length / options.branchSize)
                records = Math.floor(split.addresses.length / pages)
                remainder = split.addresses.length % pages

                offset = split.addresses.length

                paginate()
            }

            function paginate () {
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

                if (--pages > 1) paginate()
                else paginated()
            }

            var unwritten, pending
            function paginated () {
                children.unshift(full.page)
                unwritten = children.slice()
                pending = children.slice()
                writeChild()
            }

            function writeChild () {
                if (unwritten.length) writeBranch(unwritten.shift(), '.replace', check(writeChild))
                else writeParent()
            }

            function writeParent () {
                writeBranch(parent.page, '.pending', check(commit))
            }

            function commit () {
                rename(parent.page, '.pending', '.commit', check(committing))
            }

            function committing () {
                if (pending.length) replace(pending.shift(), '.replace', check(committing))
                else committed()
            }

            function committed () {
                replace(parent.page, '.commit', check(complete))
            }

            function complete () {
                release()
                shouldSplitBranch(parent.page, key, callback)
            }

            function release () {
                encached.forEach(function (page) { locker.unlock(page) })
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
            }
        }

        function drainRoot (callback) {
            var check = validator(callback, release),
                locker = new Locker,
                keys = {}, children = [], locks = [],
                root, pages, records, remainder

            locker.lock(0, true, check(partition))

            function partition (locked) {
                locks.push(root = locked)
                pages = Math.ceil(root.addresses.length / options.branchSize)
                records = Math.floor(root.addresses.length / pages)
                remainder = root.addresses.length % pages

                paginate()
            }

            function paginate () {
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

                if (--pages) paginate()
                else paginated()
            }

            var unwritten, pending
            function paginated () {
                children.reverse()

                splice('addresses', root, 0, 0, children.map(function (page) { return page.address }))

                root.addresses.slice(1).forEach(function (address) {
                    encacheEntry(root, address, keys[address])
                })

                unwritten = children.slice()
                pending = children.slice()
                writeChild()
            }

            function writeChild () {
                if (unwritten.length) writeBranch(unwritten.shift(), '.replace', check(writeChild))
                else writeRoot()
            }

            function writeRoot () {
                writeBranch(root, '.pending', check(commit))
            }

            function commit () {
                rename(root, '.pending', '.commit', check(committing))
            }

            function committing () {
                if (pending.length) replace(pending.shift(), '.replace', check(committing))
                else committed()
            }

            function committed () {
                replace(root, '.commit', check(complete))
            }

            function complete () {
                release()
                if (root.addresses.length > options.branchSize) drainRoot(callback)
                else callback(null)
            }

            function release () {
                children.forEach(function (page) { locker.unlock(page) })
                locks.forEach(function (page) { locker.unlock(root) })
                locker.dispose()
            }
        }

        function exorcise (pivot, ghostly, corporal, callback) {
            var fd, check = validator(callback)

            ok(ghostly.ghosts, 'no ghosts')
            ok(corporal.positions.length - corporal.ghosts > 0, 'no replacement')

            uncacheEntry(ghostly, splice('positions', ghostly, 0, 1).shift())
            splice('lengths', ghostly, 0, 1)
            ghostly.ghosts = 0

            fs.open(filename(ghostly.address), 'r+', 0644, check(leafOpened))

            function leafOpened (fd) {
                writePositions(fd, ghostly, check(positioned))

                function positioned () {
                    writeFooter3(fd, ghostly, check(written))
                }

                // todo: close on failure.
                function written () {
                    fs.close(fd, check(closed))
                }

                function closed () {
                    stash(corporal, corporal.ghosts, check(rekey))
                }
            }

            function rekey (entry) {
                uncacheEntry(pivot.page, pivot.page.addresses[pivot.index])
                encacheKey(pivot.page, pivot.page.addresses[pivot.index], entry.key, entry.keySize)
                callback(null, ghostly.key = entry.key)
            }
        }

        function deleteGhost (key, callback) {
            var check = validator(callback, release),
                locker = new Locker,
                descents = [],
                pivot, leaf, fd

            descents.push(pivot = new Descent(locker))
            pivot.descend(pivot.key(key), pivot.found([key]), check(upgrade))

            function upgrade () {
                pivot.upgrade(check(descendLeaf))
            }

            function descendLeaf () {
                descents.push(leaf = pivot.fork())

                leaf.descend(leaf.key(key), leaf.leaf, check(shift))
            }

            function shift () {
                exorcise(pivot, leaf.page, leaf.page, check(complete))
            }

            function complete (key) {
                release()
                callback(null, key)
            }

            function release () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
            }
        }

        function mergePages (key, leftKey, stopper, merger, ghostly, callback) {
            var check = validator(callback, release),
                locker = new Locker,
                descents = [], singles = { left: [], right: [] }, parents = {}, pages = {},
                ancestor, pivot, empties, ghosted, designation

            var keys = [ key ]
            if (leftKey) keys.push(leftKey)

            descents.push(pivot = new Descent(locker))
            pivot.descend(pivot.key(key), pivot.found(keys), check(lockPivot))

            function lockPivot () {
                var found = pivot.page.cache[pivot.page.addresses[pivot.index]].key
                if (comparator(found, keys[0]) == 0) {
                    pivot.upgrade(check(atPivot))
                } else {
                    pivot.upgrade(check(leftAboveRight))
                }
            }

            function leftAboveRight () {
                ghosted = { page: pivot.page, index: pivot.index }
                descents.push(pivot = pivot.fork())
                keys.pop()
                pivot.descend(pivot.key(key), pivot.found(keys), check(atPivot))
            }

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

            function atPivot () {
                parents.right = pivot.fork()
                parents.right.unlocker = createSingleUnlocker(singles.right)
                parents.right.descend(parents.right.key(key), stopper(parents.right), check(atRightParent))
            }

            function atRightParent () {
                parents.left = pivot.fork()
                parents.left.index--
                parents.left.unlocker = createSingleUnlocker(singles.left)
                parents.left.descend(parents.left.right,
                                     parents.left.level(parents.right.depth),
                                     check(atLeftParent))
            }

            function atLeftParent (callback) {
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
                pages.left.descend(pages.left.left, pages.left.level(parents.left.depth + 1), check(atLeftPage))
            }

            function atLeftPage (callback) {
                descents.push(pages.right = parents.right.fork())
                pages.right.descend(pages.right.left, pages.right.level(parents.right.depth + 1), check(atRightPage))
            }

            function atRightPage () {
                merger(pages, ghosted, check(merged))
            }

            function merged (dirty) {
                if (dirty) {
                    renameRightPageToMerge()
                } else {
                    release()
                    callback()
                }
            }

            function renameRightPageToMerge () {
                rename(pages.right.page, '', '.unlink', check(rewriteKeyedBranchPage))
            }

            function rewriteKeyedBranchPage () {
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

                empties = singles.right.slice(1)
                writeBranch(ancestor, '.pending', check(rewriteEmpties))
            }

            function rewriteEmpties () {
                if (empties.length) {
                    rename(empties.shift(), '', '.unlink', check(rewriteEmpties))
                } else {
                    beginCommit()
                }
            }

            function beginCommit () {
                empties = singles.right.slice(1)
                rename(ancestor, '.pending', '.commit', check(unlinkEmpties))
            }

            function unlinkEmpties () {
                if (empties.length) {
                    unlink(empties.shift(), '.unlink', check(unlinkEmpties))
                } else {
                    replaceLeftPageToMerge()
                }
            }

            function replaceLeftPageToMerge () {
                replace(pages.left.page, '.replace', check(unlinkRightPageToMerge))
            }

            function unlinkRightPageToMerge () {
                unlink(pages.right.page, '.unlink', check(endCommit))
            }

            function endCommit () {
                replace(ancestor, '.commit', check(propagate))
            }

            function propagate () {
                release()

                if (ancestor.address == 0) {
                    if (ancestor.addresses.length == 1 && !(ancestor.addresses[0] % 2)) {
                        fillRoot(callback)
                    } else {
                        callback(null)
                    }
                } else {
                    chooseBranchesToMerge(designation.key, ancestor.address, callback)
                }
            }

            function release () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                ! [ 'left', 'right' ].forEach(function (direction) {
                    if (singles[direction].length) {
                        singles[direction].forEach(function (page) { locker.unlock(page) })
                    } else {
                        locker.unlock(parents[direction].page)
                    }
                })
                locker.dispose()
            }
        }

        function mergeLeaves (key, leftKey, unbalanced, ghostly, callback) {
            function stopper (descent) { return descent.penultimate }

            function merger (leaves, ghosted, callback) {
                var check = validator(callback)

                ok(leftKey == null ||
                      comparator(leftKey, leaves.left.page.cache[leaves.left.page.positions[0]].key)  == 0,
                      'left key is not as expected')

                var left = (leaves.left.page.positions.length - leaves.left.page.ghosts)
                var right = (leaves.right.page.positions.length - leaves.right.page.ghosts)

                balancer.unbalanced(leaves.left.page, true)

                if (left + right > options.leafSize) {
                    if (unbalanced[leaves.left.page.address]) {
                        balancer.unbalanced(leaves.left.page, true)
                    }
                    if (unbalanced[leaves.right.page.address]) {
                        balancer.unbalanced(leaves.right.page, true)
                    }
                    callback(null, false)
                } else {
                    deleteGhost()
                }

                var index

                function deleteGhost () {
                    if (ghostly && left + right) {
                        if (left) {
                            exorcise(ghosted, leaves.left.page, leaves.left.page, check(merge))
                        } else {
                            exorcise(ghosted, leaves.left.page, leaves.right.page, check(merge))
                        }
                    } else {
                        merge()
                    }
                }

                function merge () {
                    leaves.left.page.right = leaves.right.page.right

                    index = leaves.right.page.ghosts

                    if (index < leaves.right.page.positions.length) fetch()
                    else rewriteLeftLeaf()
                }

                var position

                function fetch () {
                    position = leaves.right.page.positions[index]
                    stash(leaves.right.page, index, check(copy))
                }

                function copy (entry) {
                    uncacheEntry(leaves.right.page, position)

                    splice('positions', leaves.left.page, leaves.left.page.positions.length, 0, -(position + 1))
                    splice('lengths', leaves.left.page, leaves.left.page.lengths.length, 0, -(position + 1))
                    encacheEntry(leaves.left.page, -(position + 1), entry)

                    if (++index < leaves.right.page.positions.length) fetch()
                    else rewriteLeftLeaf()
                }

                function rewriteLeftLeaf () {
                    splice('positions', leaves.right.page, 0, leaves.right.page.positions.length)
                    splice('lengths', leaves.right.page, 0, leaves.right.page.lengths.length)

                    rewriteLeaf(leaves.left.page, '.replace', check(resume))
                }

                function resume () {
                    callback(null, true)
                }
            }

            mergePages(key, leftKey, stopper, merger, ghostly, callback)
        }

        function chooseBranchesToMerge (key, address, callback) {
            var check = validator(callback, release),
                locker = new Locker,
                descents = [],
                choice, lesser, greater, center

            descents.push(center = new Descent(locker))
            center.descend(center.key(key), center.address(address), check(findLeftPage))

            function goToPage (descent, address, direction, next) {
                descents.push(descent)
                descent.descend(descent.key(key), descent.address(address), check(goToSibling))

                function goToSibling () {
                    descent.index += direction == 'left' ? 1 : -1
                    descent.descend(descent[direction], descent.level(center.depth), check(next))
                }
            }

            function findLeftPage () {
                if (center.lesser != null) {
                    goToPage(lesser = new Descent(locker), center.lesser, 'right', findRightPage)
                } else {
                    findRightPage()
                }
            }

            function findRightPage () {
                if (center.greater != null) {
                    goToPage(greater = new Descent(locker), center.greater, 'left', choose)
                } else {
                    choose()
                }
            }

            function choose () {
                var choice, designator

                if (lesser && lesser.page.addresses.length + center.page.addresses.length <= options.branchSize) {
                    choice = center
                } else if (greater && greater.page.addresses.length + center.page.addresses.length <= options.branchSize) {
                    choice = greater
                }

                if (choice) {
                    descents.push(designator = choice.fork())
                    designator.descend(designator.left, designator.leaf, check(designate))
                } else {
                    release()
                    callback(null)
                }

                function designate () {
                    stash(designator.page, 0, check(propagate))
                }

                function propagate (entry) {
                    release()
                    mergeBranches(entry.key, entry.keySize, choice.page.address, callback)
                }
            }

            function release () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
            }
        }

        function mergeBranches (key, keySize, address, callback) {
            function stopper (descent) {
                return descent.child(address)
            }

            function merger (pages, ghosted, callback) {
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

                writeBranch(pages.left.page, '.replace', validate(callback, resume))

                function resume () {
                    callback(null, true)
                }
            }

            mergePages(key, null, stopper, merger, false, callback)
        }

        function fillRoot (callback) {
            var check = validator(callback), locker = new Locker, descents = [], root, child

            descents.push(root = new Descent(locker))
            root.exclude()
            root.descend(root.left, root.level(0), check(getChild))

            function getChild () {
                descents.push(child = root.fork())
                child.descend(child.left, child.level(1), check(fill))
            }

            function fill () {
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

                writeBranch(root.page, '.pending', check(rewriteChild))
            }

            function rewriteChild () {
                rename(child.page, '', '.unlink', check(beginCommit))
            }

            function beginCommit () {
                rename(root.page, '.pending', '.commit', check(unlinkChild))
            }

            function unlinkChild () {
                unlink(child.page, '.unlink', check(endCommit))
            }

            function endCommit () {
                descents.forEach(function (descent) { locker.unlock(descent.page) })
                locker.dispose()
                replace(root.page, '.commit', callback)
            }
        }

        return classify.call(this, balance, unbalanced)
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
        return function (descents, exclusive, callback) {
            var conditions, check = validator(callback)

            thrownByUser = null

            var conditions = [ descents[0].leaf, descents[0].found([key]) ]

            descents[0].descend(descents[0].key(key), function () {
                return conditions.some(function (condition) {
                    return condition()
                })
            }, check(pivotOrLeaf))

            function pivotOrLeaf(page, index) {
                if (descents[0].page.address % 2) {
                    callback(null, new Cursor(descents, false, key))
                } else {
                    descents[0].index--
                    toLeaf(descents[0].right, descents, null, exclusive, callback)
                }
            }
        }
    }

    function toLeaf (sought, descents, key, exclusive, callback) {
        var check = validator(callback)

        thrownByUser = null

        descents[0].descend(sought, descents[0].penultimate, check(penultimate))

        function penultimate() {
            if (exclusive) descents[0].exclude()
            descents[0].descend(sought, descents[0].leaf, check(leaf))
        }

        function leaf (page, index) {
            callback(null, new Cursor(descents, exclusive, key))
        }
    }

    function cursor (key, exclusive, callback) {
        var descents = [ new Descent(new Locker) ],
            check = validator(callback, function () {
                if (descents.length) {
                    descents[0].locker.unlock(descents[0].page)
                    descents[0].locker.dispose()
                }
            })
        if  (typeof key == 'function') {
            key(descents, exclusive, check(done))
        } else {
            toLeaf(descents[0].key(key), descents, key, exclusive, check(done))
        }

        function done (cursor) {
            toUserLand(callback, null, cursor)
        }
    }

    function iterator (key, callback) {
        cursor(key, false, callback)
    }

    function mutator (key, callback) {
        cursor(key, true, callback)
    }

    function balance (callback) {
        balancer.balance(validate(callback, end))

        function end () {
            toUserLand(callback)
        }
    }

    function vivify (callback) {
        var check = validator(callback), locker = new Locker, root

        locker.lock(0, false, check(begin))

        function record (address) {
            return { address: address }
        }

        function begin (page) {
            expand(page, root = page.addresses.map(record), 0, validate(callback, function () {
                release()
                toUserLand(callback, null, root)
            }, release))

            function release () {
                locker.unlock(page)
                locker.dispose()
            }
        }

        function expand (parent, pages, index, callback) {
            if (index < pages.length) {
                var address = pages[index].address
                locker.lock(address, false, check(address % 2 ? leaf : branch))
            } else {
                callback(null, pages)
            }

            function branch (page) {
                pages[index].children = page.addresses.map(record)
                if (index) designated(parent.cache[parent.addresses[index]].key)
                else keyed()

                function designated (key) {
                    pages[index].key = key
                    keyed()
                }

                function keyed () {
                    expand(page, pages[index].children, 0, validate(callback, expanded, release))
                }

                function expanded () {
                    release()
                    expand(parent, pages, index + 1, callback)
                }

                function release () {
                    locker.unlock(page)
                }
            }

            function leaf (page) {
                pages[index].children = []
                pages[index].ghosts = page.ghosts

                get(0)

                function get (recordIndex) {
                    if (recordIndex < page.positions.length) {
                        stash(page, recordIndex, validate(callback, push, release))
                    } else {
                        release()
                        expand(parent, pages, index + 1, callback)
                    }

                    function push (entry) {
                        pages[index].children.push(entry.record)
                        get(recordIndex + 1)
                    }

                    function release () {
                        locker.unlock(page)
                    }
                }
            }
        }
    }

    function purge (downTo) { magazine.purge(downTo) }

    return classify.call(this, create, open,
                               key, left, leftOf, right,
                               iterator, mutator,
                               balance, purge, vivify,
                               close,
                               _size, _nextAddress)
}

module.exports = Strata
