// Same test as unpurged key, which covered the uncovered condition of
// determining how to merge a branch page that has no left sibliing.

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
