#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, assert) {
    var strata
    step(function () {
        strata = new Strata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.create(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('a', 'a', ~ cursor.index, step())
        }, function (inserted) {
            cursor.unlock()
        })
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({
            directory: tmp,
            serialize: function (string) { return new Buffer(string) },
            deserialize: function (buffer)  { return buffer.toString() }
        })
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.get(cursor.index, step())
        }, function (got) {
            assert(got, 'a', 'inserted binary')
            cursor.unlock()
        })
    }, function () {
        strata.close(step())
    })
})
