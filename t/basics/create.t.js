require('proof')(1, require('cadence')(prove))

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
            strata.close(async())
        })
    })
}
