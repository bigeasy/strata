require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 }), ambiguity = []
    async(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async.loop([], function () {
            var index = cursor.indexOf('z', cursor.page.ghosts)
            ambiguity.unshift(cursor.page.items.length < ~index)
            if (ambiguity[0]) {
                async(function () {
                    cursor.next(async())
                }, function () {
                    return [ async.continue ]
                })
            } else {
                async(function () {
                    cursor.insert('z', 'z', ~index)
                    okay(ambiguity, [ false, true, true, true ], 'unambiguous')
                    cursor.unlock(async())
                })
            }
        }, function () {
            return [ async.break ]
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert')
    }, function() {
        strata.close(async())
    })
}
