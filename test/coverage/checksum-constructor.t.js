require('./proof')(1, prove)

function prove (async, okay) {
    var fs = require('fs'), crypto = require('crypto'), strata
    async(function () {
        fs.writeFile(tmp + '/.ignore', '', 'utf8', async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({
            directory: tmp,
            leafSize: 3,
            branchSize: 3,
            checksum: function (buffer, start, end) {
                var hash = crypto.createHash('sha1')
                hash.update(buffer.slice(start, end))
                return hash.digest('hex')
            }
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        okay(cursor.page.items.length - cursor.offset, 0, 'empty')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
