#!/usr/bin/env node

/*

    ___ usage ___ en_US ___
    stratify [options]

    options:

    -d, --directory         [name]  Name of directory to store database.

    ___ . ___

  */

require('arguable')(module, require('cadence')(function (async, program) {
    require('../t/proof').script({
        directory: program.param.directory,
        cadence: require('cadence'),
        file: program.argv.shift(),
        deepEqual: require('assert').deepEqual
    }, async())
}))

/* vim: set ts=2 sw=2: */
