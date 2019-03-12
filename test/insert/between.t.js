require('./proof')(2, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/between.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function() {
        strata.mutator('b', async())
    }, function (cursor) {
        cursor.insert('b', 'b', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/between.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'insert')
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c' ], 'records')
    }, function() {
        strata.close(async())
    })
}
