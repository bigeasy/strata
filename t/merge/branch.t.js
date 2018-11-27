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
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/branch.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'merge')
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'j', 'k', 'l', 'm', 'n' ], 'merged')
    }, function() {
        strata.close(async())
    })
}
