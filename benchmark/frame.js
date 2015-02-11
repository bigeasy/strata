var ok = require('assert').ok
var UTF8 = require('../frame/utf8')
var Binary = require('../frame/binary')
var Benchmark = require('benchmark')
var Queue = require('../queue')
var json = require('../json')

var suite = new Benchmark.Suite('frame')

var utf8 = new UTF8('none')
var binary = new Binary('none')

function createTest (framer) {
    return function () {
        var queue = new Queue
        for (var i = 0; i < 512; i++) {
            framer.serialize(json.serializer, queue, [ 1, 2, 3 ], { a: 1 })
        }
        queue.finish()
        var buffer = queue.buffers.shift(), offset = 0, count = 0
        for (;;) {
            var entry = framer.deserialize(json.deserialize, buffer, offset)
            if (entry == null) {
                break
            }
            offset += entry.length
        }
    }
}

var utf8test = createTest(utf8)
var binaryTest = createTest(binary)

utf8test()
binaryTest()

for (var i = 0; i < 1; i++)  {
    suite.add({
        name: 'utf8 ' + i,
        fn: utf8test
    })

    suite.add({
        name: 'binary ' + i,
        fn: binaryTest
    })
}

suite.on('cycle', function(event) {
    console.log(String(event.target));
})

suite.on('complete', function() {
    console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})

suite.run()
