var cadence = require('cadence')
var Staccato = require('staccato')
var Cache = require('magazine')

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
    console.log('>>>', id)
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
            console.log('here', filename)
            entries = entries.split('\n')
            entries.pop()
            entries = entries.map(function (entry) { return JSON.parse(entry) })
            entries.forEach(function (entry) {
                console.log('!', entry)
                switch (entry.method) {
                case 'add':
                    items.splice(entry.index, 0, entry.value)
                    break
                case 'remove':
                    items.splice(entry.index, 1)
                    break
                }
            })
            cartridge.value = { id: id, leaf: id % 2 == 1, items: items, ghosts: 0 }
            console.log('exiting')
            return []
        })
    })
}))

module.exports = Sheaf
