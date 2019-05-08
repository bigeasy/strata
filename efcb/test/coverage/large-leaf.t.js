require('./proof')(2, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/large-leaf.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('_', async())
    }, function (cursor) {
        cursor.insert('_', '_', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, '_abcdefghijklmnopqrstuvwxyz'.split(''), 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/large-leaf.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'split')
        strata.close(async())
    })
}
