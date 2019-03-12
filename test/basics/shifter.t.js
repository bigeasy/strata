require('proof')(3, prove)

function prove (okay) {
    var shifter = require('../../shifter')(function (buffer, start, end) { return String(end) })
    okay(shifter([]), null, 'eof')
    okay(shifter([ '0', { length: 0 }]), [ { length: 0 }, null ], 'no body')
    okay(shifter([ '0', { length: 4 }, 'a']), [ { length: 4 }, 'a' ], 'body')
}
