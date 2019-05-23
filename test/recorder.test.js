describe('recorder', () => {
    function checksum (buffer, start, end) { return String(end) }
    const assert = require('assert')
    const recorder = require('../recorder')(checksum)
    it('can format an entry with a record', () => {
        const buffer = recorder({ length: 0 }, Buffer.from('"a"'))
        assert.deepStrictEqual(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '12', '3' ], { length: 4 }, 'a' ], 'buffer')
    })
    it('can format an entry with a JSON object', () => {
        const buffer = recorder({ length: 0 }, 'a')
        assert.deepStrictEqual(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '24', '3' ], { length: 4, json: true }, 'a' ], 'string')
    })
    it('can format an entry with no body', () => {
        const buffer = recorder({ length: 0 })
        assert.deepStrictEqual(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '12' ], { length: 0 } ], 'no body')
    })
})
