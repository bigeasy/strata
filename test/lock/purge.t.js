require('./proof')(4, prove)

function prove (async, okay) {
    var strata, count = 0

    function tracer (type, object, callback) {
        switch (type) {
        case 'reference':
            if (++count == 2) {
                okay(strata.sheaf.magazine.heft > 2, 'unpurged')
                strata.purge(0)
                okay(strata.sheaf.magazine.heft, 0, 'purged')
            }
            callback()
            break
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/tree.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('h', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('h', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('i', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        strata.mutator('e', async())
    }, function (cursor) {
        cursor.remove(cursor.indexOf('e', cursor.page.ghosts))
        cursor.remove(cursor.indexOf('g', cursor.page.ghosts))
        cursor.unlock(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        okay(records, [ 'a', 'b', 'c', 'd',  'f', 'j', 'k', 'l', 'm', 'n' ], 'records')
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/tree.after.json', async())
    }, function (actual, expected) {
        okay(actual, expected, 'merge')
        strata.close(async())
    })
}
