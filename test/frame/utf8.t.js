require('proof')(6, prove)

function prove (okay) {
    var json = require('../../json')
    var Queue = require('../../queue')
    var Framer = require('../../frame/utf8')
    var framer = new Framer('sha1')
    var queue = new Queue
    var length = framer.serialize(queue, [ 1, 2, 3 ], { a: 1 }, json.serializer)
    okay(length, { length: 60, heft: 7 }, 'bodied length')
    queue.finish()
    var buffer = queue.buffers.shift()
    var entry = framer.deserialize(json.deserialize, buffer, 0, buffer.length)
    okay(framer.length(buffer, 0, buffer.length), 60, 'bodied read length')
    okay(entry, { length: 60, heft: 7, header: [ 1, 2, 3 ], body: { a: 1 } }, 'bodied')
    var queue = new Queue
    var length = framer.serialize(queue, [ 1, 2, 3 ])
    okay(length, { length: 52, heft: 0 }, 'unbodied length')
    queue.finish()
    var buffer = queue.buffers.shift()
    okay(framer.length(buffer, 0, buffer.length), 52, 'unbodied read length')
    var entry = framer.deserialize(json.deserialize, buffer, 0, buffer.length)
    okay(entry, { length: 52, heft: 0, header: [ 1, 2, 3 ], body: null }, 'unbodied')
}
