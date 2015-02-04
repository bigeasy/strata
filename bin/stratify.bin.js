#!/usr/bin/env node

/*

    ___ usage: en_US ___
    stratify [options]

    options:

    -d, --directory         [name]  Name of directory to store database.

    ___ usage ___

  */

require('arguable').parse(__filename, process.argv.slice(2), function (options) {
    require('./t/proof').script({
        directory: options.params.directory,
        cadence: require('cadence'),
        file: options.argv.shift(),
        deepEqual: require('assert').deepEqual
    }, function (error) {
        if (error) throw error
    })
})

/* vim: set ts=2 sw=2: */
