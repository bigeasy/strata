require('./proof')(2, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        assert(strata.sheaf.magazine.heft, 277, 'before purge')
        strata.purge(0)
        assert(strata.sheaf.magazine.heft, 108, 'after purge')
        cursor.unlock(async())
    }, function() {
        strata.close(async())
    })
}
