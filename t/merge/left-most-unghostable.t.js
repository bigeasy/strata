require('./proof')(4, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        okay(cursor.index, 0, 'unghostable')
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'b', 'c', 'd' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'b', 'c', 'd' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-most-unghostable.after.json', async())
    }, function (actual, expected) {
        okay.say(expected)
        okay.say(actual)

        okay(actual, expected, 'after')
        strata.close(async())
    })
}
