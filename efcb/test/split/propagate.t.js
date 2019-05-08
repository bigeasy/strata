require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/propagate.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('zz', async())
    }, function (cursor) {
        cursor.insert('zz', 'zz', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/propagate.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'split')
        strata.close(async())
    })
}
