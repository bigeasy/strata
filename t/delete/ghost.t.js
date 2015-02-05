#!/usr/bin/env node

require('./proof')(5, prove)

function prove (async, assert) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    async(function () {
        serialize(__dirname + '/fixtures/delete.json', tmp, async())
    }, function () {
        strata.open(async())
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'c', 'd' ], 'records')
    }, function () {
        strata.mutator('c', async())
    }, function (cursor) {
        async(function() {
            cursor.indexOf('c', async())
        }, function (i) {
            cursor.remove(i, async())
        }, function () {
            cursor.unlock(async())
        }, function () {
            gather(strata, async())
        })
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'deleted')
        vivify(tmp, async())
        load(__dirname + '/fixtures/ghost.after.json', async())
    }, function (actual, expected) {
        assert.say(expected)
        assert.say(actual)
        assert(actual, expected, 'directory')
        strata.close(async())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        strata.iterator('a', async())
    }, function (cursor) {
        async(function () {
            cursor.next(async())
        }, function (next) {
            assert(cursor.offset, 1, 'ghosted')
            cursor.unlock(async())
        })
    }, function () {
        gather(strata, async())
    }, function (records) {
        assert(records, [ 'a', 'b', 'd' ], 'reopened')
        strata.close(async())
    })
}
