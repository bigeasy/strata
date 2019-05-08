require('./proof')(1, prove)

function prove (async, okay) {
    var path = require('path')
    script({
        file: path.join(__dirname, 'fixtures', 'unpurged-key.txt'),
        directory: tmp,
        cadence: require('cadence'),
        okay: okay
    }, async())
}
