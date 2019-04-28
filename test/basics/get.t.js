require('proof')(5, require('cadence')(prove))

function prove (async, okay) {
    var Strata = require('../..')

    var utilities = require('../utilities')

    var options = {
        directory: utilities.directory,
        branch: { split: 5, merge: 2 },
        leaf: { split: 5, merge: 2 }
    }
    var strata = new Strata(options)

    async(function () {
        utilities.reset(utilities.directory, async())
    }, function () {
        utilities.serialize(utilities.directory, require('./fixtures/get.json'), async())
    }, function () {
        strata.open(async())
    }, function () {
        okay(strata.instance, 1, 'increment instance')
        okay(strata.journalist.magazine.heft, 0, 'json size before read')
        console.log('here')
        strata.cursor('a', async())
    }, function (cursor) {
        console.log('here')
        async(function () {
            okay(! cursor.exclusive, 'shared')
            okay(cursor.index, 0, 'index')
//            okay(cursor.offset, 0, 'offset')
            var item = cursor.items[cursor.index]
            okay(item, 'a', 'get record')
            cursor.close()
            return
            okay(item.key, 'a', 'get key')
            okay(strata._sheaf.magazine.heft, 54, 'json size after read')
            okay(item.heft, 54, 'record size')

            cursor.unlock(async())
        }, function () {
            //strata.purge(0)
            //okay(strata.sheaf.magazine.heft, 0, 'page')

            strata.close(async())
        })
    })
}
