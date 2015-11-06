#!/usr/bin/env node

/*
  ___ usage: en_US ___
  usage: node load.js

    All around tests for benchmarking Locket.
  ___ usage ___
*/

var splice = require('splice')
var cadence = require('cadence')
var path = require('path')
var crypto = require('crypto')
var seedrandom = require('seedrandom')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
var Strata = require('..')
var djb = require('./djb')
var murmur3 = require('./murmur3')
var fnv = require('./fnv')
var advance = require('advance')

var random = (function () {
    var random = seedrandom(0)
    return function (max) {
        return Math.floor(random() * max)
    }
})()

var runner = cadence(function (async) {
    var start, insert, gather
    var Binary = require('../frame/binary')
    var directory = path.join(__dirname, 'tmp'), db, count = 0
    var strata = new Strata({
        directory: directory,
        leafSize: 256,
        branchSize: 256,
        writeStage: 'leaf',
        framer: new Binary(djb)
    })

    var batches = []
    for (var j = 0; j < 7; j++) {
        var entries = []
        var type, sha, buffer, value
        for (var i = 0; i < 1024; i++) {
            var value = random(1024)
            sha = crypto.createHash('sha1')
            buffer = new Buffer(4)
            buffer.writeUInt32BE(value, 0)
            sha.update(buffer)
            var digest = sha.digest('hex')
            entries.push({
                key: digest,
                type: !! random(2) ? 'insert' : 'delete',
                record: digest
            })
        }
        batches.push(entries)
    }
    async(function () {
        rimraf(directory, async())
    }, function () {
        mkdirp(directory, async())
    }, function () {
        start = Date.now()
        strata.create(async())
    }, function () {
        var time
        var batch = 0, loop = async(function () {
            if (batch === 7) return [ loop.break ]
            splice(function (incoming, existing) {
                return incoming.type
            }, strata, advance.forward(null, batches[batch]), async())
            batch++
        })()
    }, function () {
        strata.close(async())
    }, function () {
        insert = Date.now() - start
        start = Date.now()
        strata.open(async())
    }, function () {
        var records = []
        async(function () {
            strata.iterator(strata.left, async())
        }, function (cursor) {
            var loop = async(function (more) {
                if (!more) {
                    async(function () {
                        cursor.unlock(async())
                    }, function () {
                        return [ loop.break, records ]
                    })
                } else {
                    for (var i = cursor.offset, I = cursor.length; i < I; i++) {
                        records.push(cursor.get(i).record)
                    }
                    cursor.next(async())
                }
            })(true)
        }, function () {
            strata.close(async())
        }, function () {
            gather = Date.now() - start
            console.log('insert: ' + insert + ', gather: ' + gather)
        })
    })
})

require('arguable/executable')(module, cadence(function (async, options) {
    runner(options, async())
}))
