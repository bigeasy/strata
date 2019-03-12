require('./proof')(5, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/delete.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('c', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd' ], 'deleted')
        vivify(tmp, async())
        load(__dirname + '/fixtures/ghost.after.json', async())
    }, function (actual, expected) {
        okay.say(expected)
        okay.say(actual)
        okay(actual, expected, 'directory')
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.next(async())
        }, function (next) {
            okay(cursor.offset, 1, 'ghosted')
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'd' ], 'reopened')
        strata.close(async())
    })
}
