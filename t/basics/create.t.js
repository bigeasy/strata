require('./proof')(5, prove)

function prove (async, okay) {
    var fs = require('fs'), strata
    async(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        okay(strata.sheaf.magazine.heft, 0, 'total heft')
        strata.close(async())
    }, function () {
        okay('created')
        vivify(tmp, async())
        load(__dirname + '/fixtures/create.after.json', async())
    }, function (actual, expected) {
        okay.say(actual[1])
        okay.say(expected)

        okay(actual, expected, 'written')

        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        okay(cursor.page.items.length - cursor.offset, 0, 'empty')

        cursor.unlock(async())
    }, function () {
        strata.purge(0)
        okay(strata.sheaf.magazine.heft, 0, 'purged')

        strata.close(async())
    })
}
