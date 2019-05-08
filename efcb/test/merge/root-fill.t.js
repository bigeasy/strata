require('./proof')(3, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/branch.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('h', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('i', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        strata.mutator('e', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('e', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('g', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        strata.mutator('m', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('m', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('n', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l' ], 'records')
        strata.balance(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/root-fill.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'merge')
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd', 'f', 'j', 'k', 'l' ], 'merged')
    }, function() {
        strata.close(async())
    })
}
