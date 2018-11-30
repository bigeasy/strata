var cadence = require('cadence')
var Staccato = require('staccato')
var Cache = require('magazine')

var fs = require('fs')
var path = require('path')

function Journalist (directory) {
    this._magazine = new Cache().createMagazine()
    this._directory = directory
}

Journalist.prototype.hold = function (parts) {
    var filename = path.resolve.apply(path, [ this._directory ].concat(parts))
    var cartridge = this._magazine.hold(filename, null)
    if (cartridge.value == null) {
        var stream = fs.createWriteStream(filename, { flags: 'a' })
        cartridge.value = new Staccato.Writable(stream)
    }
    return cartridge
}

Journalist.prototype.close = cadence(function (async, parts) {
    var filename = path.resolve.apply(path, [ this._directory ].concat(parts))
    var cartridge = this._magazine.hold(filename, null)
    async(function () {
        if (cartridge.value != null) {
            cartridge.value.end(async())
        }
    }, function () {
        cartridge.release()
    })
})

module.exports = Journalist
