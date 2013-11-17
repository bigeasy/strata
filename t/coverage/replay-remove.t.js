#!/usr/bin/env node

// Asserts that log replay will add and remove a record.

require('./proof')(1, function (step, tmp, Strata, deepEqual, say, gather) {
    var fs = require('fs'), strata
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.mutator('a', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('a', 'a', ~ cursor.index, step())
        }, function (inserted) {
            cursor.remove(0, step())
        }, function () {
            cursor.unlock()
        })
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [], 'empty')
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.iterator('a', step())
    }, function (cursor) {
        cursor.unlock()
    })
})
