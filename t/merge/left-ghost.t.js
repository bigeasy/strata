require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/left-ghost.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function() {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-ghost.after.json', async())
    }, function (actual, expected) {
        okay.say(expected)
        okay.say(actual)

        okay(actual, expected, 'after')
        strata.close(async())
    })
}
