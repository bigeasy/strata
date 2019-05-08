require('./proof')(4, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        var index = cursor.indexOf('b', cursor.page.ghosts)
        okay(~index <= cursor.page.items.length, 'unambiguous')
        cursor.insert('b', 'b', ~index)
        var index = cursor.indexOf('c', cursor.page.ghosts)
        okay(~index <= cursor.page.items.length, 'unambiguous cached')
        cursor.insert('c', 'c', ~index)
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function() {
        strata.close(async())
    })
}
