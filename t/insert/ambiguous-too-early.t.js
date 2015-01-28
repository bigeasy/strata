#!/usr/bin/env node

require('./proof')(3, function (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 }), ambiguity = []
    async(function () {
        serialize(__dirname + '/fixtures/ambiguous.before.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n' ], 'records')
    }, function () {
        strata.mutator('a', async())
    }, function (cursor) {
        var page
        var page = async(function () {
            cursor.indexOf('z', async())
        }, function (index) {
            ambiguity.unshift(cursor.length < ~index)
            if (ambiguity[0]) {
                cursor.next(async(page(), 0))
            } else {
                async(function () {
                    cursor.insert('z', 'z', ~index, async())
                }, function () {
                    assert(ambiguity, [ 0, 1, 1, 1 ], 'unambiguous')
                    cursor.unlock(async())
                })
            }
        })(1)
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'd', 'f', 'g', 'h', 'i', 'l', 'm', 'n', 'z' ], 'records after insert')
    }, function() {
        strata.close(async())
    })
})
