require('proof')(5, async (okay) => {
    function checksum (buffer, start, end) { return String(end - start) }
    const recorder = require('../recorder')(checksum)
    const Player = require('../player')
    {
        const player = new Player(checksum)
        okay(player != null, 'constructed')
    }
    {
        const player = new Player(checksum)
        const buffers = [
            recorder({ value: 1 }),
            recorder({}, { value: 1 }),
            recorder({}, Buffer.from('abcdefghijklm\nnopqrstuvwxyz'))
        ]
        const buffer = Buffer.concat(buffers)
        okay(player.split(buffer.slice(0, 5)), [], 'partial')
        okay(!player.empty(), 'player has remainder')
        okay(player.split(buffer.slice(5, 120)), [{
            header: { value: 1 },
            body: null,
            sizes: [ 46 ]
        }, {
            header: {},
            body: { value: 1 },
            sizes: [ 37, 11 ]
        }], 'body partial')
        okay(player.split(buffer.slice(120)).map(function (entry) {
            entry.body = entry.body.toString().split('\n')
            return entry
        }), [{
            header: {},
            body: [ 'abcdefghijklm', 'nopqrstuvwxyz' ],
            sizes: [ 38, 27 ]
        }], 'body remainder')
    }
})
