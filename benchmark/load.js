#!/usr/bin/env node

/*
  ___ usage: en_US ___
  usage: node load.js

    All around tests for benchmarking Locket.
  ___ usage ___
*/

var advance = require('advance')
var splice = require('splice')
var cadence = require('cadence/redux')
var path = require('path')
var crypto = require('crypto')
var seedrandom = require('seedrandom')
var rimraf = require('rimraf')
var mkdirp = require('mkdirp')
var Strata = require('..')

var random = (function () {
    var random = seedrandom(0)
    return function (max) {
        return Math.floor(random() * max)
    }
})()

var runner = cadence(function (async) {
    var directory = path.join(__dirname, 'tmp'), db, count = 0
    var extractor = function (record) { return record.key }
    var strata = new Strata({
        directory: directory,
        extractor: extractor,
        leafSize: 256,
        branchSize: 256,
        writeStage: 'leaf'
    })
    async(function () {
        rimraf(directory, async())
    }, function () {
        mkdirp(directory, async())
    }, function () {
        strata.create(async())
    }, function () {
        var batch = 0
        var loop = async(function () {
            if (batch++ == 7) return [ loop ]
            var entries = []
            var type, sha, buffer, value
            for (var i = 0; i < 1024; i++) {
                var value = random(1024)
                sha = crypto.createHash('sha1')
                buffer = new Buffer(4)
                buffer.writeUInt32BE(value, 0)
                sha.update(buffer)
                entries.push({
                    key: sha.digest('hex'),
                    type: !! random(2) ? 'insert' : 'delete'
                })
            }
            var iterator = advance(entries, function (record, callback) {
                callback(null, record, record.key)
            })
            splice(function (incoming, existing) {
                return incoming.record.type
            }, strata, iterator, async())
        })()
    }, function () {
        strata.close(async())
    }, function () {
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
                        return [ loop, records ]
                    })
                } else {
                    for (var i = cursor.offset, I = cursor.length; i < I; i++) {
                        records.push(cursor.get(i).record)
                    }
                    cursor.next(async())
                }
            })(true)
        }, function () {
            console.log('count', records.length)
            strata.close(async())
        })
    })
})

require('arguable/executable')(module, cadence(function (async, options) {
    runner(options, async())
    return
    AsyncProfile.profile(function () {
        runner(options, function (error) { if (error) throw error })
    })
}))
