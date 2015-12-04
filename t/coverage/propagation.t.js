require('./proof')(1, prove)

function prove (async, assert) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        try {
            strata.iterator('a', function (error, cursor) {
                cursor.unlock(function () {})
                throw new Error('propagated')
            })
        } catch (e) {
            assert(e.message, 'propagated', 'propagated error')
            strata.close(async())
        }
    })
}
