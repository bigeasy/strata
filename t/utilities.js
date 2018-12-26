var fs = require('fs')
var path = require('path')

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
                    console.log('elsed!')
                } else {
                    entries = entries.map(function (entry) { return entry.value.id })
                }
                vivified[file] = entries
            })
        })
    }, function () {
        return [ vivified ]
    })
})
