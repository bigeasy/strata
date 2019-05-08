require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/empties-pivot.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('ay', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        console.log('before balance')
        strata.balance(async())
    }, function () {
        console.log('after balance')
        vivify(tmp, async())
        load(__dirname + '/fixtures/empties-pivot.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'after')
        strata.close(async())
    })
}
