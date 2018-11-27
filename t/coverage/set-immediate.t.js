require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/../basics/fixtures/get.json', tmp, async())
    }, function () {
        strata = createStrata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            setImmediate: true
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        okay(cursor.page.items[cursor.offset].record, 'a', 'get')
        cursor.unlock(async())
    })
}
