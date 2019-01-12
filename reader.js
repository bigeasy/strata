var Staccato = require('staccato')
var fs = require('fs')
var byline = require('byline')
var cadence = require('cadence')
var Interrupt = require('interrupt').createInterrupter('b-tree')

function Reader (file) {
    this.readable = new Staccato.Readable(byline(fs.createReadStream(file), { keepEmptyLines: true }))
}

Reader.prototype.read = cadence(function (async) {
    var record = { checksum: '', header: {}, body: null }
    async(function () {
        this.readable.read(async())
    }, function (checksum) {
        record.checksum = JSON.parse(checksum)
        async(function () {
            this.readable.read(async())
        }, function (header) {
            if (header == null) {
                return [ null ]
            }
            record.header = JSON.parse(header)
            if (record.header.length == 0) {
                return record
            }
            var remaining = record.header.length - 1, buffers = []
            async.loop([], function () {
                this.readable.read(async())
            }, function (buffer) {
                Interrupt.assert(buffer != null, 'truncated')
                buffers.push(buffer)
                remaining -= buffer.length
                if (remaining == 0) {
                    record.body = Buffer.concat(buffers)
                    if (record.header.json) {
                        record.body = JSON.parse(record.body.toString())
                    }
                    return [ async.break, record ]
                }
                buffers.push(Buffer.from('\n'))
                remaining--
            })
        })
    })
})

module.exports = Reader
