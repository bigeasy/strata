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
                cursor.insert('y', 'y', ~cursor.indexOf('y', cursor.index))
                cursor.insert('z', 'z', ~cursor.indexOf('z', cursor.index))
                okay(cursor.indexOf('x', cursor.ghosts), 0, 'inserted')
                okay(~cursor.indexOf('a', 0), 0, 'not found')
                // TODO Assert heft and purge again.
                okay(strata.journalist.magazine.heft, 130, 'heft')
            }, function () {
                cursor.flush(async())
            })
        }, function () {
            strata.close(async())
        }, function () {
            utilities.vivify(utilities.directory, async())
        }, function (x) {
            okay(x, {
                '0.0': [ '0.1' ],
                '0.1': [{
                    method: 'insert', index: 0, body: { key: 'x', value: 'x' }
                }, {
                    method: 'insert', index: 1, body: { key: 'y', value: 'y' }
                }, {
                    method: 'insert', index: 2, body: { key: 'z', value: 'z' }
                }]
            }, 'inserted')
        })
    })
}
