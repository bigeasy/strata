require('proof')(3, prove)

function prove (okay) {
    var recorder = require('../../recorder')(function (buffer, start, end) { return String(end) })
    var buffer = recorder({ length: 0 }, Buffer.from('"a"'))
    okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
        return JSON.parse(line)
    }), [ '17', { length: 4 }, 'a' ], 'buffer')
    var buffer = recorder({ length: 0 }, 'a')
    okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
        return JSON.parse(line)
    }), [ '17', { length: 4 }, 'a' ], 'string')
    var buffer = recorder({ length: 0 })
    okay(buffer.toString().split(/\n/).slice(0, -1).map(function (line) {
        return JSON.parse(line)
    }), [ '13', { length: 0 } ], 'no body')
}
