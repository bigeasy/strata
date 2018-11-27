require('./proof')(2, prove)

function prove (async, okay) {
    var strata
    var Queue = require('../../queue')
    var Scribe = require('../../scribe')
    var json = require('../../json')
    var cadence = require('cadence')

    var writeUserRecord = cadence(function (async, strata) {
        var locker = strata.sheaf.createLocker()
        async(function () {
            locker.lock(1, true, async())
        }, [function (page) {
            locker.unlock(page)
            locker.dispose()
        }], function (page) {
            var appender = strata.logger.createAppender(page)
            appender.writeUserRecord([ 1 ], { a: 1 })
            appender.close(async())
        })
    })
    async(function () {
        serialize(__dirname + '/fixtures/get.json', tmp, async())
    }, function () {
        strata = createStrata({
            directory: tmp, leafSize: 3, branchSize: 3,
            userRecordHandler: function (entry) {
                okay(entry.header, [ 1 ], 'header')
                okay(entry.body, { a: 1 }, 'body')
            }
        })
        strata.open(async())
    }, function () {
        writeUserRecord(strata, async())
    }, function () {
        strata.close(async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({
            directory: tmp, leafSize: 3, branchSize: 3
        })
        strata.open(async())
    }, function () {
        strata.close(async())
    })
}
