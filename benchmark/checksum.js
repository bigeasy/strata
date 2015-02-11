var ok = require('assert').ok
var checksum = require('../_checksum')
var murmur3 = require('./murmur3')
var fnv = require('./fnv')
var djb = require('./djb')
var Benchmark = require('benchmark').Benchmark
var crypto = require('crypto')

var suite = new Benchmark.Suite('frame')

var buffer = crypto.randomBytes(1024)

function djbTest () {
    djb(buffer, 0, buffer.length)
}

function fnvTest () {
    fnv(buffer, 0, buffer.length)
}

function murmur3Test () {
    murmur3(buffer, 0, buffer.length)
}

var sha1 = checksum('sha1', true)
function sha1Test () {
    sha1(buffer, 0, buffer.length)
}

var md5 = checksum('md5', true)
function md5Test () {
    md5(buffer, 0, buffer.length)
}

var sha1hex = checksum('sha1')
function sha1hexTest () {
    sha1hex(buffer, 0, buffer.length)
}

var md5hex = checksum('md5')
function md5hexTest () {
    md5hex(buffer, 0, buffer.length)
}

djbTest()
fnvTest()
murmur3Test()
sha1Test()
md5Test()

for (var i = 0; i < 1; i++)  {
    suite.add({
        name: 'djbTest ' + i,
        fn: djbTest
    })

    suite.add({
        name: 'fnvTest ' + i,
        fn: fnvTest
    })

    suite.add({
        name: 'murmur3Test ' + i,
        fn: murmur3Test
    })

    suite.add({
        name: 'sha1 ' + i,
        fn: sha1Test
    })

    suite.add({
        name: 'md5 ' + i,
        fn: md5Test
    })

    suite.add({
        name: 'sha1hex ' + i,
        fn: sha1hexTest
    })

    suite.add({
        name: 'md5hex ' + i,
        fn: md5hexTest
    })
}

suite.on('cycle', function(event) {
    console.log(String(event.target));
})

suite.on('complete', function() {
    console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})

suite.run()
