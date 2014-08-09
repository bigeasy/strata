#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, serialize, gather, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), ambiguity = []
    step(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        var page
        var page = step(function () {
            cursor.indexOf('z', step())
        }, function (index) {
            cursor.insert('z', 'z', ~index, step())
        }, function (unambiguous) {
            ambiguity.unshift(unambiguous)
            if (ambiguity[0]) {
                cursor.next(step(page(), 0))
            } else {
                assert(ambiguity, [ 0, 1, 1, 1 ], 'unambiguous')
                cursor.unlock(step())
            }
        })(1)
    }, function () {
        gather(strata, step())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert')
    }, function() {
        strata.close(step())
    })
})
