require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        strata.iterator('h', async())
        cursor.unlock(async())
    }, function (cursor) {
        okay(cursor.page.items[cursor.offset].record, 'h', 'got')
        cursor.unlock(async())
    }, function() {
        strata.close(async())
    })
}
