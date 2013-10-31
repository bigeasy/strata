require('./proof')(1, function (step, tmp, cadence, script, deepEqual) {
    var path = require('path')
    script({
        file: path.join(__dirname, 'fixtures', 'unpurged-key.txt'),
        directory: tmp,
        cadence: cadence,
        deepEqual: deepEqual
    }, step())
})
