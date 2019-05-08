require('./proof')(3, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        async(function () {
            cursor.remove(cursor.index)
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        async(function () {
            okay(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(async())
        }, function () {
            vivify(tmp, async())
            load(__dirname + '/fixtures/merge.after.json', async())
        }, function (actual, expected) {
            okay.say(expected)
            okay.say(actual)

            okay(actual, expected, 'merge')
        }, function () {
            gather(strata, async())
        }, function (records) {
            okay(records, [ 'a', 'c', 'd' ], 'records')
            strata.balance(async())
        })
    })
}
