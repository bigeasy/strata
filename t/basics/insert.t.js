require('proof')(4, require('cadence')(prove))

function prove (async, okay) {
    var Strata = require('../..')

    var rimraf = require('rimraf')
    var mkdirp = require('mkdirp')
    var path = require('path')
    var directory = path.resolve(__dirname, '../tmp')

    var utilities = require('../utilities')

    var options = {
        directory: directory,
        branch: { split: 5, merge: 2 },
        leaf: { split: 5, merge: 2 }
    }
    var strata = new Strata(options)

    async(function () {
        rimraf(directory, async())
    }, function () {
        mkdirp(directory, async())
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
                okay(strata._sheaf.heft, 4, 'heft')
            })
        }, function () {
            strata.close(async())
        }, [function () {
            utilities.vivify(directory, async())
        }, function (error) {
            console.log(error.stack)
        }], function (x) {
            console.log(x)
            return [ async.break ]
            console.log('closed')
            okay(x, {}, /*require('./fixtures/inserted'), */'inserted')
        })
    })
}
