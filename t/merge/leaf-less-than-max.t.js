require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index)
            cursor.next(async())
        }, function () {
            cursor.remove(cursor.indexOf('d', cursor.page.ghosts))
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'c' ], 'records')
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/leaf-less-than-max.after.json', async())
    }, function (expected, actual) {
        okay.say(expected)
        okay.say(actual)

        okay(actual, expected, 'merge')
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'c' ], 'merged')
        strata.close(async())
    })
}
