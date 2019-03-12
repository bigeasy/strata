require('./proof')(5, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/exorcise.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function() {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd', 'e', 'f', 'g' ], 'merged')
        vivify(tmp, async())
        load(__dirname + '/fixtures/exorcise.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'after')
        strata.close(async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            okay(cursor.page.right, { key: 'd', address: 5 }, 'referring leaf updated')
            cursor.next(async())
        }, function () {
            okay(cursor.page.items[0].key, 'd', 'key deleted')
        }, function () {
            cursor.unlock(async())
        })
    })
}
