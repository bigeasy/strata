describe('player', () => {
    function checksum (buffer, start, end) { return String(end - start) }
    const assert = require('assert')
    const recorder = require('../recorder')(checksum)
    const Player = require('../player')
    it('can be constructed', () => {
        const player = new Player(checksum)
        assert(player != null, 'constructed')
    })
    it('can split lines', () => {
        var player = new Player(checksum)
        const buffers = [
            recorder({ value: 1 }),
            recorder({}, { value: 1 }),
            recorder({}, Buffer.from('abcdefghijklm\nnopqrstuvwxyz'))
        ]
        const buffer = Buffer.concat(buffers)
        assert.deepStrictEqual(player.split(buffer.slice(0, 5)), [], 'partial')
        assert(!player.empty(), 'player has remainder')
        assert.deepStrictEqual(player.split(buffer.slice(5, 120)), [{
            checksums: [ '22' ],
            header: { value: 1, length: 0 },
            body: null,
            sizes: [ 22 ]
        }, {
            checksums: [ '25', '11' ],
            header: { json: true, length: 12 },
            body: { value: 1 },
            sizes: [ 25, 11 ]
        }], 'body partial')
        assert.deepStrictEqual(player.split(buffer.slice(120)).map(function (entry) {
            entry.body = entry.body.toString().split('\n')
            return entry
        }), [{
            checksums: [ '13', '27' ],
            header: { length: 28 },
            body: [ 'abcdefghijklm', 'nopqrstuvwxyz' ],
            sizes: [ 13, 27 ]
        }], 'body remainder')
    })
})