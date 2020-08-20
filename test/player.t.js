require('proof')(5, async (okay) => {
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
            recorder({ value: 1 }, []),
            recorder({}, [ Buffer.from('a'), Buffer.from('b') ])
        ]
        const buffer = Buffer.concat(buffers)
        okay(player.split(buffer.slice(0, 5)), [], 'partial')
        okay(!player.empty(), 'player has remainder')
        const [ one, two ] = player.split(buffer.slice(5, 120))
        okay(one, {
            header: { value: 1 },
            parts: [],
            sizes: [ 7, 36 ]
        }, 'no parts')
        two.parts = two.parts.map(buffer => buffer.toString())
        okay(two, {
            header: {},
            parts: [ 'a', 'b' ],
            sizes: [ 7, 30, 2, 2 ]
        }, 'parts')
    }
})
