var fs = require('fs')
var path = require('path')

var shifter = require('../shifter')(null)

var cadence = require('cadence')

exports.vivify = cadence(function (async, directory) {
    var vivified = {}
    async(function () {
        fs.readdir(path.resolve(directory, 'pages'), async())
    }, function (files) {
        async.forEach([ files ], function (file) {
            if (!/^\d+$/.test(file)) {
                return [ async.continue ]
            }
            async(function () {
                fs.readFile(path.resolve(directory, 'pages', file, 'append'), 'utf8', async())
            }, function (entries) {
                entries = entries.split(/\n/)
                entries.pop()
                entries = entries.map(function (entry) { return JSON.parse(entry) })
                if (+file % 2 == 1) {
                    var records = []
                    while (entries.length != 0) {
                        var record = shifter(entries), header = record[0]
                        switch (header.method) {
                        case 'insert':
                            records.push({ method: header.method, index: header.index, body: record[1] })
                            break
                        case 'remove':
                            records.push({ method: header.method, index: header.index })
                            break
                        }
                    }
                    vivified[file] = records
                } else {
                    var records = []
                    while (entries.length != 0) {
                        var record = shifter(entries), header = record[0]
                        switch (header.method) {
                        case 'insert':
                            records.splice(header.index, 0, header.value.id)
                            break
                        }
                    }
                    vivified[file] = records
                }
            })
        })
    }, function () {
        return [ vivified ]
    })
})
