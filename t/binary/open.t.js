#!/usr/bin/env node

require('./proof')(2, function (Strata, tmp, step, equal) {
    var fs = require('fs'), strata
    step(function () {
        // we can put this in a function that populates a configuration with default
        // Strata binary configurations.
        strata = new Strata({
            directory: tmp,
            delimiter: [ 0xfedcba09, 0x87654321 ],
            count: {
                size: 8,
                extractor: function (buffer) { return buffer.readDoubleLE(0) }
            },
            leafSize: 3,
            branchSize: 3
        })
        strata.create(step())
    }, function () {
        strata.close(step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        equal(strata.size, 0, 'json size')
        equal(strata.nextAddress, 2, 'next address')
    })
})
