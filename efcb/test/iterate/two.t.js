require('./proof')(5, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 }), records = []
    async(function () {
        serialize(__dirname + '/fixtures/two.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            okay(cursor.index, 0, 'found')
            okay(cursor.offset, 0, 'found')
            okay(cursor.page.items.length, 2, 'length')
            var record = cursor.page.items[cursor.index].record
            records.push(record)
            okay(cursor.index, 0, 'same index')
            var record = cursor.page.items[cursor.index + 1].record
            records.push(record)
            okay(records, [ 'a', 'b' ], 'records')
            cursor.unlock(async())
        }, function () {
            strata.close(async())
        })
    })
}
