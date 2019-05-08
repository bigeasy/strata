require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (a) {
        async(function () {
            a.insert('a', 'a', ~a.index)
            strata.mutator('b', async())
            a.unlock(async())
        }, function (b) {
            async(function () {
                b.insert('b', 'b', ~b.index)
                b.unlock(async())
            }, function () {
                gather(strata, async())
            })
        })
    }, function (records) {
        okay(records, [ 'a', 'b' ], 'records')
        strata.close(async())
    })
}
