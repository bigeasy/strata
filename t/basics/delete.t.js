require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('c', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'd' ], 'records')

        strata.purge(0)
        okay(strata.sheaf.magazine.heft, 0, 'purged')

        strata.close(async())
    })
}
