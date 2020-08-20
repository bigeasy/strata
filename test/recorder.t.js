require('proof')(5, (okay) => {
    function checksum (buffer, start, end) { return end }
    const recorder = require('../recorder')(checksum)
    // format an entry with a record
    {
        const buffer = recorder({ operation: 'test' }, [ Buffer.from('"a"'), Buffer.from('"a"') ])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 68, 8 ], { header: { operation: 'test' }, length: [ 4, 4 ], json: [ false, false ] }, 'a', 'a' ], 'buffer')
    }
    // format an entry with a JSON object
    {
        const buffer = recorder({ operation: 'test' }, [ 'a', 'a' ])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 66, 8 ], { header: { operation: 'test' }, length: [ 4, 4 ], json: [ true, true ] }, 'a', 'a' ], 'string')
    }
    //format an entry with no body
    {
        const buffer = recorder({ operation: 'test' }, [ 'a' ])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 59, 4 ], { json: [ true ], length: [ 4 ], header: { operation: 'test' } }, 'a' ], 'no body')
    }
    // format an entry with a JSON key buffer body
    {
        const buffer = recorder({ operation: 'test' }, [ 'a', Buffer.from('"a"') ])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 67, 8 ], { header: { operation: 'test' }, length: [ 4, 4 ], json: [ true, false ] }, 'a', 'a' ], 'string')
    }
    // format an entry with no key or body
    {
        const buffer = recorder({ operation: 'test' }, [])
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ 54, 0 ], { header: { operation: 'test' }, length: [], json: [] } ], 'empty')
    }
    return
})
