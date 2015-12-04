require('./proof')(2, prove)

function prove (async, assert) {
    var fs = require('fs'), path = require('path'), strata
    async(function () {
        fs.mkdir(path.join(tmp, 'pages'), 0755, async())
    }, function () {
        fs.writeFile(path.join(tmp, 'pages', '0.0'), '6 x_x\n', 'utf8', async())
    }, function () {
        strata = createStrata({ directory: tmp })
        strata.open(async())
    }, [function () {
        strata.iterator('a', async())
    }, function (error) {
        assert(error.message, 'corrupt line: could not find end of line header', 'cannot find header')
    }], function () {
        fs.writeFile(path.join(tmp, 'pages', '0.0'), '6 x 0\n', 'utf8', async())
    }, function () {
        strata = createStrata({ directory: tmp })
        strata.open(async())
    }, [function () {
        strata.iterator('a', async())
    }, function (error) {
        assert(error.message, 'corrupt line: invalid checksum', 'invalid checksum')
    }])
}
