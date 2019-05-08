require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/right-ghost.after.json', async())
    }, function (actual, expected) {
        okay.say(expected)
        okay.say(actual)

        okay(actual, expected, 'after')
        strata.close(async())
    })
}
