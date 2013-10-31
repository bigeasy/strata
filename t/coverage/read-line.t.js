#!/usr/bin/env node

require('./proof')(2, function (step, Strata, deepEqual, tmp, insert, load, objectify, equal) {
    var fs = require('fs'), strata
    step(function () {
        fs.writeFile(tmp + '/0', '6 x_x\n', 'utf8', step())
    }, function () {
        strata = new Strata({ directory: tmp })
        strata.open(step())
    }, [function () {
        strata.iterator('a', step())
    }, function (_, error) {
        equal(error.message, 'corrupt line: could not find end of line header', 'cannot find header')
    }], function () {
        fs.writeFile(tmp + '/0', '6 x 0\n', 'utf8', step())
    }, function () {
        strata = new Strata({ directory: tmp })
        strata.open(step())
    }, [function () {
        strata.iterator('a', step())
    }, function (_, error) {
        equal(error.message, 'corrupt line: invalid checksum', 'invalid checksum')
    }])
})
