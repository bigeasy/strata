#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata
    async(function () {
        strata = new Strata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.create(async())
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.insert('a', 'a', ~ cursor.index, async())
        }, function (inserted) {
            cursor.unlock(async())
        })
    }, function () {
        strata.close(async())
    }, function () {
        strata = new Strata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        assert(cursor.get(cursor.index).record, 'a', 'inserted binary')
        cursor.unlock(async())
    }, function () {
        strata.close(async())
    })
}
