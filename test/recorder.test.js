require('proof')(3, (okay) => {
    function checksum (buffer, start, end) { return String(end) }
    const recorder = require('../recorder')(checksum)
    // format an entry with a record
    {
        const buffer = recorder({ length: 0 }, Buffer.from('"a"'))
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '47', '3' ], { json: false, length: 4, header: { length: 0 } }, 'a' ], 'buffer')
    }
    // format an entry with a JSON object
    {
        const buffer = recorder({ length: 0 }, 'a')
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '46', '3' ], { length: 4, json: true, header: { length: 0 } }, 'a' ], 'string')
    }
    //format an entry with no body
    {
        const buffer = recorder({ length: 1 })
        okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
            return JSON.parse(line)
        }), [ [ '47' ], { json: false, length: 0, header: { length: 1 } } ], 'no body')
    }
})
