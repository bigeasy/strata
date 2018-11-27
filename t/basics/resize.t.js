require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/resize.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('b', async())
    }, function (cursor) {
        okay(cursor.page.items[cursor.index].record, 'b', 'loaded')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
