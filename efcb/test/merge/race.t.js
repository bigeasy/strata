require('./proof')(2, prove)

function prove (async, okay) {
    var cadence = require('cadence'), strata, insert

    function tracer (type, object, callback) {
        switch (type) {
        case 'plan':
            cadence(function (async) {
                async(function () {
                    strata.mutator(insert, async())
                }, function (cursor) {
                    cursor.insert(insert, insert, ~cursor.index)
                    cursor.unlock(async())
                })
            })(callback)
            break
        default:
            callback()
        }
    }

    async(function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('b', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        insert = 'b'
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/race-left.after.json', async())
    }, function(actual, expected) {
        okay(actual, expected, 'race left')
        strata.close(async())
    }, function () {
        serialize(__dirname + '/fixtures/race.before.json', tmp, async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer })
        strata.open(async())
    }, function () {
        strata.mutator('d', async())
    }, function (cursor) {
        cursor.remove(cursor.index)
        cursor.unlock(async())
    }, function () {
        insert = 'd'
        strata.balance(async())
    }, function () {
        vivify(tmp, async())
        load(__dirname + '/fixtures/race-right.after.json', async())
    }, function(actual, expected) {
        okay(actual, expected, 'race right')
        strata.close(async())
    })
}
