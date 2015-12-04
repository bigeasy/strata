require('./proof')(2, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/first.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/first.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.vivify(async())
    }, function (result) {
        assert(result, [ { address: 1, children: [ 'b', 'c' ], ghosts: 0 } ], 'ghostbusters')
    }, function () {
        strata.close(async())
    })
}
