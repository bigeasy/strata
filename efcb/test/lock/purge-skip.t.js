require('./proof')(2, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        okay(strata.sheaf.magazine.heft, 277, 'before purge')
        strata.purge(0)
        okay(strata.sheaf.magazine.heft, 108, 'after purge')
        cursor.unlock(async())
    }, function() {
        strata.close(async())
    })
}
