#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata
    async(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
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
})
