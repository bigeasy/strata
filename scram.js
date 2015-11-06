// bogus function to get 100 test coverage.
var cadence = require('cadence')

module.exports = cadence(function (async, entry, f) {
    async([function () {
        f.call(this, async())
    }, function (error) {
        entry.scram(async())
        throw error
    }])
})
