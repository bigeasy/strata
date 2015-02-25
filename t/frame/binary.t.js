#!/usr/bin/env node

require('proof')(6, prove)

function prove (assert) {
    var json = require('../../json')
    var Queue = require('../../queue')
    var Framer = require('../../frame/binary')
    var framer = new Framer('sha1')
    var queue = new Queue
    var length = framer.serialize(queue, [ 1, 2, 3 ], { a: 1 }, json.serializer)
    assert(length, { length: 51, heft: 7 }, 'bodied length')
    queue.finish()
    var buffer = queue.buffers.shift()
    assert(framer.length(buffer, 0, buffer.length), 51, 'bodied read length')
    var entry = framer.deserialize(json.deserialize, buffer, 0, buffer.length)
    assert(entry, { length: 51, heft: 7, header: [ 1, 2, 3 ], body: { a: 1 } }, 'bodied')
    var queue = new Queue
    var length = framer.serialize(queue, [ 1, 2, 3 ])
    assert(length, { length: 44, heft: 0 }, 'unbodied length')
    queue.finish()
    var buffer = queue.buffers.shift()
    assert(framer.length(buffer, 0, buffer.length), 44, 'unbodied read length')
    var entry = framer.deserialize(json.deserialize, buffer, 0, buffer.length)
    assert(entry, { length: 44, heft: null, header: [ 1, 2, 3 ], body: null }, 'unbodied')
}
