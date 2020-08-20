const assert = require('assert')
const EOL = Buffer.from('\n')

class Player {
    constructor (checksum) {
        this._checksum = checksum
        this._remainder = Buffer.alloc(0)
        this._entry = {
            state: 'checksum',
            header: null,
            checksums: null,
            sizes: []
        }
    }

    split (chunk) {
        const entries = []
        let { state, header, checksums, sizes } = this._entry, start = 0
        let offset = this._remainder.length
        chunk = Buffer.concat([ this._remainder, chunk ])
        SPLIT: for (;;) {
            switch (state) {
            case 'checksum': {
                    const index = chunk.indexOf(0xa, offset)
                    if (!~index) {
                        break SPLIT
                    }
                    sizes.push(index - start + 1)
                    checksums = JSON.parse(chunk.slice(start, index + 1))
                    start = offset = index + 1
                    state = 'header'
                }
                break
            case 'header': {
                    const index = chunk.indexOf(0xa, offset)
                    if (!~index) {
                        break SPLIT
                    }
                    sizes.push(index - start + 1)
                    const buffer = chunk.slice(start, index + 1)
                    assert.equal(checksums[0], this._checksum.call(null, buffer, 0, buffer.length))
                    header = JSON.parse(buffer.toString())
                    state = 'payload'
                    start = offset = index + 1
                }
                break
            case 'payload': {
                    const length = header.lengths.reduce((sum, value) => sum + value, 0)
                    if (chunk.length - start < length) {
                        break SPLIT
                    }
                    const checksum = this._checksum.call(null, chunk, start, start + length)
                    assert.equal(checksums[1], checksum)
                    const parts = []
                    for (const length of header.lengths) {
                        sizes.push(length)
                        let part = chunk.slice(start, start + length - 1)
                        offset = start = start + length
                        parts.push(part)
                    }
                    entries.push({
                        header: header.header,
                        parts: parts,
                        sizes: sizes
                    })
                    sizes = []
                    state = 'checksum'
                }
                break
            }
        }
        this._remainder = chunk.slice(start)
        this._entry = { state, start, header, checksums, sizes }
        return entries
    }

    _split (chunk) {
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
                assert(checksum(buffer, 0, buffer.length) === this._entry.checksums[0])
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
