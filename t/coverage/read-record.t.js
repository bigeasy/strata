require('./proof')(1, function (step, Strata, tmp, serialize, equal) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3, tracer: tracer }), path = require('path')

    function tracer (type, object, callback) {
        if (type == 'readRecord') {
            callback(new Error('bogus error'))
        } else {
            callback()
        }
    }

    step(function () {
        serialize(path.join(__dirname, '/fixtures/split-race.before.json'), tmp, step())
    }, function () {
        strata.open(step())
    },[function () {
        strata.iterator('a', step())
    }, function (_, error) {
        equal(error.message, 'bogus error', 'error on read record')
    }], function () {
        strata.close(step())
    })
})
