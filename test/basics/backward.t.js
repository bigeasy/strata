require('./proof')(5, prove)

function prove (async, okay) {
    var strata, right
    async(function () {
        serialize(__dirname + '/fixtures/merge.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.mutator(strata.leftOf('c'), async())
    }, function (cursor) {
        okay(cursor.exclusive, 'exclusive')
        right = cursor.page.right.address
        okay(cursor.page.items[0].record, 'a', 'go left')
        cursor.unlock(async())
    }, function () {
        strata.mutator(strata.leftOf('d'), async())
    }, function (cursor) {
        okay(cursor.page.address, right, 'address and right')
        okay(!cursor.exclusive, 'shared')
        okay(cursor.page.items[0].record, 'c', 'go left missing')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
