require('./proof')(5, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/one.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            okay(cursor.index, 0, 'found')
            okay(cursor.offset, 0, 'offset')
            okay(cursor.page.ghosts, 0, 'ghosts')
            okay(cursor.page.items.length, 1, 'length')
            okay(cursor.page.items[cursor.index].record, 'a', 'records')
            cursor.unlock(async())
        }, function () {
            strata.close(async())
        })
    })
}
