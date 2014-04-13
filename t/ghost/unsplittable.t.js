#!/usr/bin/env node

require('./proof')(3, function (step, Strata, tmp, load, serialize, objectify, gather, deepEqual) {
    var strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
    step(function () {
        serialize(__dirname + '/fixtures/unsplittable.before.json', tmp, step())
    }, function () {
        strata.open(step())
    }, function () {
        strata.mutator('g', step())
    }, function (cursor) {
        step(function () {
            cursor.insert('g', 'g', ~cursor.index, step())
        }, function () {
            cursor.indexOf('d', step())
        }, function (index) {
            cursor.remove(index, step())
        }, function () {
            cursor.unlock()
            gather(step, strata)
        })
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'records')
        strata.balance(step())
    }, function () {
        gather(step, strata)
    }, function (records) {
        deepEqual(records, [ 'a', 'b', 'c', 'e', 'f', 'g' ], 'balanced')
        objectify(tmp, step())
        load(__dirname + '/fixtures/unsplittable.after.json', step())
    }, function (actual, expected) {
        deepEqual(actual, expected, 'after')
        strata.close(step())
    })
})
