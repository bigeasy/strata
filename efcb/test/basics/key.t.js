require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator(strata.key('a'), async())
    }, function (cursor) {
        okay(cursor.page.items[cursor.offset].record, 'a', 'got')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
