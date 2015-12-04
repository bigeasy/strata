require('./proof')(1, prove)

function prove (async, assert) {
    var strata, value = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU'
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.mutator(value, async())
    }, function (cursor) {
        cursor.insert(value, value, ~cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records[0], value, 'done')
        strata.close(async())
    })
}
