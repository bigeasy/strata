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
            header: { value: 1 },
            body: null,
            sizes: [ 46 ]
        }, {
            header: {},
            body: { value: 1 },
            sizes: [ 37, 11 ]
        }], 'body partial')
        assert.deepStrictEqual(player.split(buffer.slice(120)).map(function (entry) {
            entry.body = entry.body.toString().split('\n')
            return entry
        }), [{
            header: {},
            body: [ 'abcdefghijklm', 'nopqrstuvwxyz' ],
            sizes: [ 38, 27 ]
        }], 'body remainder')
    })
})
