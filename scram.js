// bogus function to get 100 test coverage.
var cadence = require('cadence/redux')

module.exports = cadence(function (step, entry, f) {
    step([function () {
        f.call(this, step())
    }, function (error) {
        entry.scram(step())
        throw error
    }])
})
