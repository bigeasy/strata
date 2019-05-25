const assert = require('assert')
const EOL = Buffer.from('\n')

class Player {
    constructor (checksum) {
        this._remainder = Buffer.alloc(0)
        this._checksum = checksum
        this._entry = {
            checksums: null,
            header: null,
            push: { header: null, body: null, sizes: [] }
        }
    }

    split (chunk) {
        const buffer = Buffer.concat([ this._remainder, chunk ])
        const buffers = []
        let begin = 0
        for (let i = 0, I = buffer.length; i < I; i++) {
            if (buffer[i] == 0xa) {
                buffers.push(buffer.slice(begin, i))
                begin = i + 1
            }
        }
        this._remainder = buffer.slice(begin)
        const entries = [], checksum = this._checksum
        buffers.forEach((buffer) => {
            if (this._entry.checksums == null) {
                this._entry.checksums = JSON.parse(buffer.toString())
            } else if (this._entry.header == null) {
                assert(checksum(buffer, 0, buffer.length) == this._entry.checksums[0])
                this._entry.header = JSON.parse(buffer.toString())
                this._entry.push.header = this._entry.header.header
                this._entry.push.sizes.push(buffer.length)
                if (this._entry.header.length == 0) {
                    entries.push(this._entry.push)
                    this._entry = {
                        checksums: null,
                        header: null,
                        push: { header: null, body: null, sizes: [] }
                    }
                } else {
                    this._entry.push.body = []
                }
            } else {
                this._entry.push.body.push(buffer)
                const length = this._entry.push.body.reduce(function (length, buffer) {
                    return length + buffer.length
                }, 0) + 1
                if (length < this._entry.header.length) {
                    this._entry.push.body.push(EOL)
                } else {
                    const body = Buffer.concat(this._entry.push.body)
                    assert(checksum(body, 0, body.length) == this._entry.checksums[1])
                    if (this._entry.header.json) {
                        this._entry.push.body = JSON.parse(body.toString())
                    } else {
                        this._entry.push.body = body
                    }
                    this._entry.push.sizes.push(body.length)
                    entries.push(this._entry.push)
                    this._entry = {
                        checksums: null,
                        header: null,
                        push: { header: null, body: null, sizes: [] }
                    }
                }
            }
        })
        return entries
    }

    empty () {
        return this._remainder.length == 0
    }
}

module.exports = Player
