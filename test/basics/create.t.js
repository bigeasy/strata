require('proof')(6, require('cadence')(prove))

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
            utilities.vivify(directory, async())
        }, function (x) {
            okay(x, require('./fixtures/created'), 'created')
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
                var index = cursor.indexOf('z', cursor.index)
                okay(~index, 1, 'insert indexOf index')
                cursor.insert('z', 'z', ~index)
            })
        }, function () {
            strata.cursor('x', async())
        }, function (cursor) {
            async([function () {
                cursor.close()
            }], function () {
                okay({
                    index: cursor.index,
                    sought: cursor.sought,
                    found: cursor.found,
                    value: cursor.items[cursor.index].record
                }, {
                    index: 0,
                    sought: 'x',
                    found: true,
                    value: 'x'
                }, 'cursor found')
            })
        })
    })
}
