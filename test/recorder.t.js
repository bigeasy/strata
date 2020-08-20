require('proof')(2, (okay) => {
    function checksum (buffer, start, end) { return end }
    const recorder = require('../recorder')(checksum)
    // format an entry with a record
    {
        const buffer = recorder({ operation: 'test' }, [ Buffer.from('"a"'), Buffer.from('"a"') ])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 48, 8 ], { header: { operation: 'test' }, lengths: [ 4, 4 ] }, 'a', 'a' ], 'buffer')
    }
    // format an entry with no key or body
    {
        const buffer = recorder({ operation: 'test' }, [])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 45, 0 ], { header: { operation: 'test' }, lengths: [] } ], 'empty')
    }
})
