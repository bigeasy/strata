require('./proof')(3, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/unsplittable.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('g', async())
    }, function (cursor) {
        cursor.insert('g', 'g', ~cursor.index)
        cursor.remove(cursor.indexOf('d', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records')
        strata.balance(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'balanced')
        vivify(tmp, async())
        load(__dirname + '/fixtures/unsplittable.after.json', async())
    }, function (actual, expected) {
        assert(actual, expected, 'after')
        strata.close(async())
    })
}
