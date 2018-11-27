require('./proof')(1, prove)

function prove (async, okay) {
    var strata
    async(function () {
        strata = createStrata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        cursor.insert('a', 'a', ~cursor.index)
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        okay(cursor.page.items[cursor.index].record, 'a', 'inserted binary')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
