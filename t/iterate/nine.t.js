#!/usr/bin/env node

require('./proof')(4, function (step, Strata, tmp, serialize, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/nine.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        assert(cursor.index, 0, 'index')
        assert(cursor.offset, 0, 'offset')
        assert(cursor.length, 3, 'length')
        cursor.unlock()
    }, function () {
        gather(step, strata)
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i' ], 'records')
    }, function() {
        strata.close(step())
    })
})
