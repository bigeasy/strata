var ok = require('assert').ok
var fs = require('fs')
var path = require('path')
var cadence = require('cadence')

function Player (options) {
    this.directory = options.directory
    this.framer = options.framer
    this.deserializers = options.deserializers
    this.userRecordHandler = options.userRecordHandler
}

// TODO outgoing
Player.prototype.io = cadence(function (async, direction, filename) {
    async(function () {
        fs.open(filename, direction[0], async())
    }, function (fd) {
        async(function () {
            fs.fstat(fd, async())
        }, function (stat) {
            var io = cadence(function (async, buffer, position) {
                var offset = 0

                var length = stat.size - position
                var slice = length < buffer.length ? buffer.slice(0, length) : buffer

                async.loop([ 0 ], function (count) {
                    if (count < slice.length - offset) {
                        offset += count
                        fs[direction](fd, slice, offset, slice.length - offset, position + offset, async())
                    } else {
                        return [ async.break, slice, position ]
                    }
                })
            })
            return [ fd, stat, io ]
        })
    })
})

Player.prototype.read = cadence(function (async, sheaf, page) {
    page.entries = page.ghosts = 0
    var rotation = 0
    async.loop([], [function () {
        var filename = path.join(this.directory, 'pages', page.address + '.' + rotation)
        this.io('read', filename, async())
    }, function (error) {
        if (rotation === 0 || error.code !== 'ENOENT') {
            throw error
        }
        return [ async.break, page ]
    }], function (fd, stat, read) {
        page.position = 0
        page.rotation = rotation++
        this.play(sheaf, fd, stat, read, page, async())
    })
})

Player.prototype._play = function (sheaf, slice, start, page) {
    var leaf = page.address % 2 === 1,
        deserialize = leaf ? this.deserializers.record : this.deserializers.key,
        framer = this.framer
    for (var i = 0, I = slice.length; i < I; i += entry.length) {
        var entry = framer.deserialize(deserialize, slice, i, I)
        if (entry == null) {
            return i
        }
        var header = entry.header
        ok(Math.abs(header[0]) === ++page.entries, 'entry count is off')
        if (header[0] < 0) {
            if (page.position === 0) {
                page.right = {
                    address: header[1] || null,
                    key: entry.body || null
                }
                if (header[2] === 0 && page.ghosts) {
                    page.splice(0, 1)
                    page.ghosts = 0
                }
            } else if (this.userRecordHandler != null) {
                var handler = this.userRecordHandler
                entry.header.shift()
                handler(entry)
            }
        } else {
            var index = header[1]
            if (leaf) {
                // TODO see if it is faster to perform the slices here directly.
                if (index > 0) {
                    page.splice(index - 1, 0, {
                        key: sheaf.extractor(entry.body),
                        record: entry.body,
                        heft: entry.length
                    })
                } else if (~index === 0 && page.address !== 1) {
                    ok(!page.ghosts, 'double ghosts')
                    page.ghosts++
                } else if (index < 0) {
                    page.splice(-(index + 1), 1)
                }
            } else {
                var address = header[2], key = null, heft = 0
                if (index - 1) {
                    key = entry.body
                    heft = entry.length
                }
                page.splice(index - 1, 0, {
                    key: key, address: address, heft: heft
                })
            }
        }
        page.position += entry.length
    }
    return i
}

Player.prototype.play = cadence(function (async, sheaf, fd, stat, read, page) {
    var buffer = new Buffer(sheaf.options.readLeafStartLength || 1024 * 1024)
    // TODO really want to register a cleanup without an indent.
    async([function () {
        fs.close(fd, async())
    }], function () {
        async.loop([ buffer, 0 ], function (buffer, position) {
            read(buffer, position, async())
        }, function (slice, start) {
            var offset = this._play(sheaf, slice, start, page)
            if (start + buffer.length < stat.size) {
                if (offset == 0) {
                    buffer = new Buffer(buffer.length * 2)
                    read(buffer, start, async())
                } else {
                    read(buffer, start + offset, async())
                }
            } else {
                return [ async.break ]
            }
        })
    })
})

module.exports = Player
