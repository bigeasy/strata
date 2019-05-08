require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/empty.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.mutator('f', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.mutator('i', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/empty.after.json', async())
    }, function (actual, expected) {
        console.log(require('util').inspect(actual, false, null))
        console.log(require('util').inspect(expected, false, null))
        okay(actual, expected, 'after balance')
        strata.close(async())
    })
}
