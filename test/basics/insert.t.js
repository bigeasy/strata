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
        var strata = new Strata(options)
        async(function () {
            strata.create(async())
        }, function () {
            strata.cursor('x', async())
        }, function (cursor) {
            async([function () {
                cursor.close()
            }], function () {
                okay({
                    sought: cursor.sought,
                    found: cursor.found
                }, {
                    sought: 'x',
                    found: cursor.found
                }, 'cursor not found')
                cursor.insert('x', 'x', cursor.index)
                okay(cursor.indexOf('x', cursor.ghosts), 0, 'inserted')
                okay(~cursor.indexOf('z', 0), 1, 'not found')
                // TODO Assert heft and purge again.
                okay(strata._sheaf.magazine.heft, 3, 'heft')
            }, function () {
                cursor.flush(async())
            })
        }, function () {
            strata.close(async())
        }, function () {
            utilities.vivify(utilities.directory, async())
        }, function (x) {
            okay(x, {
                0: [ 1 ],
                1: [{ method: 'insert', index: 0, body: 'x' }]
            }, 'inserted')
        })
    })
}
