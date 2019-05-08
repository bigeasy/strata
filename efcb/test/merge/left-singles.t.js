require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/left-singles.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('bt', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('bt', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('bu', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        strata.mutator('bw', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('bw', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/left-singles.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'merged')
    }, function () {
        strata.close(async())
    })
}
