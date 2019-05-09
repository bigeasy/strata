require('./proof')(4, prove)

function prove (async, okay) {
    var Strata = require('../..')

    var utilities = require('../utilities')

    var options = {
        directory: utilities.directory,
        branch: { split: 5, merge: 2 },
        leaf: { split: 5, merge: 2 }
    }
    var strata = new Strata(options)
    var strata
    async(function () {
        utilities.reset(utilities.directory, async())
    }, function () {
        utilities.serialize(utilities.directory, require('./fixtures/split.before.json'), async())
    }, function () {
        strata.open(async())
    }, function () {
        strata.cursor('b', async())
    }, function (cursor) {
        async([function () {
            cursor.close()
        }], function () {
            cursor.insert('b', 'b', cursor.index)
            cursor.flush(async())
        })
    }, function () {
        strata.flush(async())
    })
}
