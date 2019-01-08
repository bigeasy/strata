var Staccato = require('staccato')
var recorder = require('./recorder')(function () { return '0' })
var cadence = require('cadence')

function Appender (file) {
    this.writable = new Staccato.Writable(file, { flags: 'a' })
}

Appender.prototype.append = cadence(function (async, header, body) {
    this.writable.write(recorder(header, body), async())
})

Appender.prototype.end = cadence(function (async) {
    this.writable.end(async())
})
