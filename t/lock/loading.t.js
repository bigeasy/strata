require('./proof')(2, prove)

function prove (async, okay) {
    var strata
    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('h', async())
        strata.iterator('h', async())
    }, function (first, second) {
        okay(first.page.items[first.offset].record, 'h', 'first')
        first.unlock(async())
        okay(second.page.items[second.offset].record, 'h', 'second')
        second.unlock(async())
    }, function() {
        strata.close(async())
    })
}
