require('proof')(2, prove)

function prove (okay) {
    var recorder = require('../../recorder')(function (buffer, start, end) { return String(end) })
    var buffer = recorder({ length: 0 }, Buffer.from('"a"\n'))
    okay(buffer.toString().split(/\n/).splice(0, 3).map(function (line) {
        return JSON.parse(line)
    }), [ '17', { length: 4 }, 'a' ], 'buffer')
    var buffer = recorder({ length: 0 }, 'a')
    okay(buffer.toString().split(/\n/).splice(0, 3).map(function (line) {
        return JSON.parse(line)
    }), [ '17', { length: 4 }, 'a' ], 'string')
}
