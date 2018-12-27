require('proof')(9, require('cadence')(prove))

function prove (async, okay) {
    var Strata = require('../..')

    var options = {
        directory: utilities.directory,
        branch: { split: 5, merge: 2 },
        leaf: { split: 5, merge: 2 }
    }
    var strata = new Strata(options)

    async(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        okay(strata.sheaf.magazine.heft, 0, 'json size before read')
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            okay(! cursor.exclusive, 'shared')
            okay(cursor.index, 0, 'index')
            okay(cursor.offset, 0, 'offset')
            var item = cursor.page.items[cursor.offset]
            okay(item.record, 'a', 'get record')
            okay(item.key, 'a', 'get key')
            okay(strata.sheaf.magazine.heft, 54, 'json size after read')
            okay(item.heft, 54, 'record size')

            cursor.unlock(async())
        }, function () {
            strata.purge(0)
            okay(strata.sheaf.magazine.heft, 0, 'page')

            strata.close(async())
        })
    })
}
