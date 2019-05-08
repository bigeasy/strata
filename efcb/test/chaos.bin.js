require('arguable')(module, require('cadence')(function (async, program) {
    var seed = 0
    var Strata = require('..')
    var seedrandom = require('seedrandom')
    var rng = seedrandom(seed)

    var stop = false

    function value (max, min) {
        return rng() * (max - min) + min;
    }

    var strata = new Strata({
        leafSize: 64,
        branchSize: 64,
        extractor: function (record) { return record.value }
    })

    var action
    for (;;) {
        if (stop) {
            break
        }
        var action = value(1000000, 0)
        if (action < 1) {
            // reopen
            console.log('reopen')
        } else if (action < 500000) {
            console.log('search')
        } else if (action < 700000) {
            console.log('delete')
        } else {
            console.log('insert')
        }
    }

    process.on('shutdown', function () { stop = true })
}))
