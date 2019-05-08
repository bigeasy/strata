require('./proof')(1, prove)

function prove (async, okay) {
    var fs = require('fs'), path = require('path')
    var strata = createStrata({ directory: tmp, checksum: 'none' })
    async(function () {
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        fs.readFile(path.join(tmp, 'pages', '0.0'), 'utf8', async())
    }, function (body) {
        okay(+(body.split(/\n/)[0].split(/\s+/)[1]), 0, 'zero')
    })
}
