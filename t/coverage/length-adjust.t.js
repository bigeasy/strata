#!/usr/bin/env node

require('./proof')(1, function (step, Strata, tmp, gather, equal) {
    var strata, value = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU'
    step(function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(step())
    }, function () {
        strata.mutator(value, step())
    }, function (cursor) {
        step(function () {
            cursor.insert(value, value, ~ cursor.index, step())
        }, function (inserted) {
            cursor.unlock()
        })
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        gather(strata, step())
    }, function (records) {
        equal(records[0], value, 'done')
        strata.close(step())
    })
})
