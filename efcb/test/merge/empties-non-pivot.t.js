require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/empties-non-pivot.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('cr', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/empties-non-pivot.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'after')
        strata.close(async())
    })
}
