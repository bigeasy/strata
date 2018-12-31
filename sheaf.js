var cadence = require('cadence')
var Staccato = require('staccato')
var Cache = require('magazine')

var shifter = require('./shifter')(function () { return '0' })

var Signal = require('signal')

var restrictor = require('restrictor')

var Turnstile = require('turnstile')

var fs = require('fs')
var path = require('path')

function Sheaf (directory) {
    this.magazine = new Cache().createMagazine()
    this._directory = directory
    this.turnstile = new Turnstile
}

Sheaf.prototype.load = restrictor.enqueue('canceled', cadence(function (async, id) {
    var cartridge = this.magazine.hold(id, null)
    async([function () {
        cartridge.release()
    }], function () {
        if (cartridge.value != null) {
            return [ async.return ]
        }
        var filename = path.resolve(this._directory, 'pages', String(id), 'append')
        var items = []
        async(function () {
            fs.readFile(filename, 'utf8', async())
        }, function (entries) {
            entries = entries.split('\n')
            entries.pop()
            entries = entries.map(function (entry) { return JSON.parse(entry) })
            while (entries.length) {
                var record = shifter(entries), header = record[0]
                switch (header.method) {
                case 'insert':
                    if (id % 2 == 0) {
                        items.splice(header.index, 0, header.value)
                    } else {
                        items.splice(header.index, 0, record[1])
                    }
                    break
                case 'remove':
                    items.splice(header.index, 1)
                    break
                }
            }
            cartridge.value = { id: id, leaf: id % 2 == 1, items: items, ghosts: 0 }
            return []
        })
    })
}))

module.exports = Sheaf
