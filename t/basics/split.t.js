require('./proof')(4, prove)

function prove (async, assert) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        cursor.insert('b', 'b', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.after.json', async())
    }, function(actual, expected) {
        assert.say(actual)
        assert.say(expected)

        assert(actual, expected, 'split')

        strata.purge(0)
        assert(strata.sheaf.magazine.heft, 0, 'purged')

        assert(Object.keys(strata.sheaf.lengths).length, 'not balanced')

        strata.close(async())
    })
}
