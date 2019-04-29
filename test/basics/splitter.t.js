require('proof')(4, prove)

function prove (okay) {
    function checksum (buffer, start, end) { return String(end - start) }
    var recorder = require('../../recorder')(checksum)
    var Splitter = require('../../splitter')
    var splitter = new Splitter(checksum)
    var buffers = [
        recorder({ value: 1 }),
        recorder({}, { value: 1 }),
        recorder({}, Buffer.from('abcdefghijklm\nnopqrstuvwxyz'))
    ]
    var buffer = Buffer.concat(buffers)
    okay(splitter.split(buffer.slice(0, 5)), [], 'partial')
    okay(!splitter.empty(), 'splitter has remainder')
    okay(splitter.split(buffer.slice(5, 120)), [{
        checksums: [ '22' ],
        header: { value: 1, length: 0 },
        body: null,
        sizes: [ 22 ]
    }, {
        checksums: [ '25', '11' ],
        header: { json: true, length: 12 },
        body: { value: 1 },
        sizes: [ 25, 11 ]
    }], 'body partial')
    okay(splitter.split(buffer.slice(120)).map(function (entry) {
        entry.body = entry.body.toString().split('\n')
        return entry
    }), [{
        checksums: [ '13', '27' ],
        header: { length: 28 },
        body: [ 'abcdefghijklm', 'nopqrstuvwxyz' ],
        sizes: [ 13, 27 ]
    }], 'body remainder')
}
