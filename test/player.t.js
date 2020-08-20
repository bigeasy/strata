require('proof')(7, async (okay) => {
    function checksum (buffer, start, end) { return end - start }
    const recorder = require('../recorder')(checksum)
    const Player = require('../player')
    {
        const player = new Player(checksum)
        okay(player != null, 'constructed')
    }
    {
        const player = new Player(checksum)
        const buffers = [
            recorder({ value: 1 }, [ 'a' ]),
            recorder({}, [ Buffer.from('"a"'), { value: 1 } ]),
            recorder({}, [ { value: 1 }, Buffer.from('abcdefghijklm\nnopqrstuvwxyz') ]),
            recorder({ value: 1 }, [])
        ]
        const buffer = Buffer.concat(buffers)
        okay(player.split(buffer.slice(0, 5)), [], 'partial')
        okay(!player.empty(), 'player has remainder')
        const one = player.split(buffer.slice(5, 120)).shift()
        okay(one, {
            header: { value: 1 },
            parts: [ 'a' ],
            sizes: [ 7, 50, 4 ]
        }, 'no body')
        const remaining = player.split(buffer.slice(120))
        const two = remaining.shift()
        okay({
            header: two.header,
            key: two.parts[0].toString(),
            body: two.parts[1],
            sizes: two.sizes,
        }, {
            header: {},
            key: '"a"',
            body: { value: 1 },
            sizes: [ 8, 50, 4, 12 ]
        }, 'buffer key')
        const three = remaining.shift()
        okay({
            header: three.header,
            key: three.parts[0],
            body: three.parts[1].toString().split('\n'),
            sizes: three.sizes,
        }, {
            header: {},
            key: { value: 1 },
            body: [ 'abcdefghijklm', 'nopqrstuvwxyz' ],
            sizes: [ 8, 51, 12, 28 ]
        }, 'buffer body')
        const four = remaining.shift()
        okay({
            header: four.header,
            parts: four.parts,
            sizes: four.sizes,
        }, {
            header: { value: 1 },
            parts: [],
            sizes: [ 7, 45 ]
        }, 'no parts')
    }
})
