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
            async([function () {
                console.log('>>>', path.resolve(directory, 'pages', file))
                async(function () {
                    try {
                    fs.readFileSync(path.resolve(directory, 'pages', file), 'utf8')
                    } catch (e) {
                        console.log(e.stack)
                    }
                    fs.readFile(path.resolve(directory, 'pages', file), 'utf8', async())
                }, function () {
                    console.log('done')
                })
            }, function (error) {
                console.log('error')
                console.log(error.stack)
                throw error
            }], function (entries) {
                entries = entries.split(/\n/)
                entries.pop()
                entries = entries.map(function (entry) { return JSON.parse(entry) })
                console.log(entries, file, +file % 2)
                if (+file % 2 == 1) {
                } else {
                    entries = entries.map(function (entry) { return entry.value.id })
                console.log(entries)
                }
                vivified[file] = entries
            })
        })
    }, function () {
        return [ vivified ]
    })
})
