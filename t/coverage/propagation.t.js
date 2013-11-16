require('./proof')(1, function (step, Strata, equal, tmp) {
    var strata
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        try {
            strata.iterator('a', function (error, cursor) {
                cursor.unlock()
                throw new Error('propagated')
            })
        } catch (e) {
            equal(e.message, 'propagated', 'propagated error')
            strata.close(step())
        }
    })
})
