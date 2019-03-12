// Asserts that log replay will skip over the positions array.

require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.insert('a', 'a', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a' ], 'written')
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3, replay: true })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        cursor.unlock(async())
    })
}
