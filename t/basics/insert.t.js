require('./proof')(4, prove)

function prove (async, okay) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        okay(cursor.exclusive, 'exclusive')
        cursor.insert('a', 'a', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        okay(strata.sheaf.magazine.heft, 54, 'json size')
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/insert.json', async())
    }, function (actual, expected) {
        okay.say(expected)
        okay.say(actual)

        okay(actual, expected, 'insert')

        okay.say(expected.segment00000001)

        strata.purge(0)
        okay(strata.sheaf.magazine.heft, 0, 'purged')

        strata.close(async())
    })
}
