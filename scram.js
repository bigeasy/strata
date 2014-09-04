// bogus function to get 100 test coverage.
var cadence = require('cadence')

module.exports = cadence(function (step, entry, f) {
    step([function () {
        f.call(this, step())
    }, function (errors) {
        entry.scram(step())
        throw errors
    }])
})
