require('./proof')(4, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/split.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.insert('g', 'g', ~cursor.indexOf('g', cursor.page.ghosts))
        cursor.insert('h', 'h', ~cursor.indexOf('h', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'e', 'f', 'g', 'h' ], 'records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'after tree')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'e', 'f', 'g', 'h' ], 'balanced records')
        vivify(tmp, async())
        load(__dirname + '/fixtures/split.balanced.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'after balance')
        strata.close(async())
    })
}
