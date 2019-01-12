require('proof')(3, require('cadence')(prove))

function prove (async, okay) {
    var Appender = require('../../appender')
    var Reader = require('../../reader')

    var path = require('path')

    var utilities = require('../utilities')

    async(function () {
        utilities.reset(utilities.directory, async())
    }, function () {
        var appender = new Appender(path.resolve(utilities.directory, 'file'))
        async(function () {
            appender.append({ value: 1 }, async())
        }, function () {
            appender.append({ value: 2 },  { value: 3 }, async())
        }, function () {
            appender.append({ value: 4 },  Buffer.from('a\nb\n'), async())
        }, function () {
            appender.end(async())
        })
    }, function () {
        var reader = new Reader(path.resolve(utilities.directory, 'file'))
        async(function () {
            reader.read(async())
        }, function (record) {
            okay(record, { checksum: '0', header: { value: 1, length: 0 }, body: null }, 'no body')
            reader.read(async())
        }, function (record) {
            okay(record, {
                checksum: '0',
                header: { value: 2, json: true, length: 12 },
                body: { value: 3 }
            }, 'json body')
            reader.read(async())
        }, function (record) {
            record.body = record.body.toString()
            okay(record, {
                checksum: '0',
                header: { value: 4, length: 5 },
                body: 'a\nb\n'
            }, 'buffer body')
        })
    })
}
