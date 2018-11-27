// TODO Outgoing. We're going to use a new version of Proof that does not have
// this sort of thing.
var fs = require('fs'),
    path = require('path'),
    crypto = require('crypto'),
    cadence = require('cadence'),
    Strata = require('..'),
    rimraf = require('rimraf'),
    path = require('path'),
    ok = require('assert').ok

function check (callback, forward) {
    return function (error, result) {
        if (error) callback(error)
        else forward(result)
    }
}

function vivify (directory, callback) {
    var files, dir = {}, lengths = {}, count = 0

    directory = path.join(directory, 'pages')
    fs.readdir(directory, check(callback, list))

    function list ($1) {
        (files = $1).forEach(function (file) {
            if (!/^\./.test(file)) readFile(file)
            else read()
        })
    }

    function readFile (file) {
        dir[file] = []
        lengths[file] = []

        fs.readFile(path.resolve(directory, file), 'utf8', check(callback, lines))


        function lines (lines) {
            lines = lines.split(/\n/)
            lines.pop()
            lines.forEach(function (line, index) {
                var $ = /^\d+\s[\da-f]+\s(\S+)(?:\s(.*))?$/.exec(line)
                var record = { header: JSON.parse($[1]) }
                if ($[2]) {
                    record.body = JSON.parse($[2])
                }
                dir[file].push(record)
                lengths[file][index] = line.length + 1
            })
            read()
        }
    }

    function read () {
        if (++count == files.length) callback(null, renumber(order(abstracted(dir, lengths))))
    }
}

// TODO pretty print should be in here, so I can use it from stratify and the
// stringify utility.
function stringify (directory, callback) {
    vivify(directory, check(callback, segments))

    function segments (segments) {
        callback(null, JSON.stringify(segments, null, 2))
    }
}

function load (segments, callback) {
    fs.readFile(segments, 'utf8', check(callback, parse))

    function parse (json) {
        callback(null, renumber(order(JSON.parse(json))))
    }
}

var gather = cadence(function (async, strata) {
    var records = [], page, item
    if (typeof strata == 'function') throw new Error
    async(function () {
        strata.iterator(strata.left, async())
    }, function (cursor) {
        async.loop([ true ], function (more) {
            if (!more) {
                async(function () {
                    cursor.unlock(async())
                }, function () {
                    return [ async.break, records ]
                })
            } else {
                for (var i = cursor.offset; i < cursor.page.items.length; i ++) {
                    records.push(cursor.page.items[i].record)
                }
                cursor.next(async())
            }
        })
    })
})

var serialize = cadence(function (async, segments, directory) {
    async([function () {
        fs.mkdir(path.join(directory, 'drafts'), 0755, async())
        directory = path.join(directory, 'pages')
        fs.mkdir(directory, 0755, async())
    }, function (error) {
        if (error.code !== 'EEXIST') {
            throw error
        }
    }], function () {
        if (typeof segments == 'string') load(segments, async())
        else return [ segments ]
    }, function (json) {
        var dir = directivize(json)
        var files = Object.keys(dir)
        var count = 0

        async.forEach([ files ], function (file) {
            var records = []
            dir[file].forEach(function (line) {
                var record = [ JSON.stringify(line.header) ]
                var hash = crypto.createHash('sha1')
                hash.update(record[0])
                if (line.body) {
                    var body = JSON.stringify(line.body)
                    hash.update(' ', 'utf8')
                    hash.update(body)
                    record.push(body)
                }
                record.unshift(hash.digest('hex'))
                record = record.join(' ')

                var length = record.length + 1
                var entire = length + String(length).length + 1
                length = Math.max(entire, length + String(entire).length + 1)

                records.push(length + ' ' + record)
            })
            records = records.join('\n') + '\n'
            fs.writeFile(path.resolve(directory, String(file) + '.0'), records, 'utf8', async())
        })
    })
})

function abstracted (dir) {
    var output = {}
    var position = 0
    var grouped = {}

    for (var file in dir) {
        var address = file.split('.').shift()
        var files = grouped[address]
        if (!files) {
            files = grouped[address] = []
        }
        files.push({ name: file, body: dir[file] })
    }

    for (var address in grouped) {
        var record
        if (address % 2) {
            var files = grouped[address].sort(function (a, b) {
                a = a.name.split('.').pop()
                b = b.name.split('.').pop()
                return +(a) - +(b)
            })
//            console.log(files.map(function (file) { return file.name }))
            record = { log: [] }
            var entry = 0
            files.forEach(function (file) {
                position = 0
                file.body.forEach(function (line, index) {
//                    console.log(line)
                    var json = line.header
                    ok(++entry == Math.abs(json[0]), 'entry record is wrong')
                    if (json[0] >= 0) {
                        if (json[1] > 0) {
                            record.log.push({ type: 'add', value: line.body })
                        } else {
                            record.log.push({ type: 'del', index: Math.abs(json[1]) - 1 })
                        }
                    } else {
                        ok(json[0] < 0, 'meta records should have negative entry')
                        ok(index == 0, 'header not first entry')
                        if (json[1]) record.right = Math.abs(json[1])
                    }
                })
            })
        } else {
            var children = []
            grouped[address][0].body.forEach(function (json, index) {
                if (json.header[1] > 0) {
                    children.splice(json.header[1] - 1, 0, json.header[2])
                } else {
                    children.splice(~json.header[1], 1)
                }
            })
            record = { children: children }
        }
        output[address] = record
    }

    return output
}

function renumber (json) {
    var addresses = Object.keys(json)
                          .map(function (address) { return + address })
                          .sort(function (a, b) { return +(a) - +(b) })

    var next = 0
    var map = {}
    addresses.forEach(function (address) {
        while ((address % 2) != (next % 2)) next++
        map[address] = next++
    })

    var copy = {}
    for (var address in json)  {
        var object = json[address]
        if (address % 2) {
            object.right && (object.right = map[object.right])
        } else {
            object.children = object.children.map(function (address) {
                return map[address]
            })
        }
        copy[map[address]] = json[address]
    }

    return copy
}

function order (json) {
    for (var address in json) {
        var object = json[address]
        if (address % 2) {
            var order = []
            object.log.forEach(function (entry) {
                var index
                switch (entry.type) {
                case 'add':
                    for (index = 0; index < order.length; index++) {
                        if (order[index] > entry.value) {
                            break
                        }
                    }
                    order.splice(index, 0, entry.value)
                    break
                case 'del':
                    if (!entry.index && !object.ghost) {
                        object.ghost = order[0]
                    }
                    order.splice(entry.index, 1)
                    break
                }
            })
            object.order = order
        }
    }
    return json
}

function directivize (json) {
    var directory = {}, keys = {}

    function key (address) {
        var object = json[address]
        if (object.children) {
            return key(object.children[0])
        } else {
            return object.ghost || object.order[0]
        }
    }

    var checksum = 40

    for (var address in json) {
        var object = json[address]
        if (object.children) {
            directory[address] = object.children.map(function (address, index) {
                return { header: [ index + 1, index + 1, address ], body: index ? key(address) : null }
            })
        } else {
            var ghosts = 0
            var positions = []
            var lengths = []
            var position = 0
            var order = []
            var records = 0
            directory[address] = object.log.filter(function (entry) {
                return entry.type != 'pos'
            }).map(function (entry, count) {
                var record
                var index
                switch (entry.type) {
                case 'add':
                    records++
                    for (index = 0; index < order.length; index++) {
                        if (order[index] > entry.value) {
                            break
                        }
                    }
                    order.splice(index, 0, entry.value)
                    positions.splice(index, 0, position)
                    record = { header: [ count + 2, index + 1 ], body: entry.value }
                    break
                case 'del':
                    records--
                    record = { header: [ count + 2, -(entry.index + 1) ] }
                    break
                }
                var length = JSON.stringify(record.header).length + 1 + checksum + 1
                if (record.body != null) {
                    length += JSON.stringify(record.body).length + 1
                }
                var entire = length + String(length).length + 1
                length = Math.max(entire, length + String(entire).length + 1)
                switch (entry.type) {
                case 'add':
                    lengths.splice(index, 0, length)
                    break
                }
                position += length
                return record
            })
            directory[address].unshift({
                header: [ -1, object.right || 0, 0 ],
                body: object.right ? key(object.right) : null
            })
        }
    }

    return directory
}

function createStrata (options) {
    if (!options.comparator) {
        options.comparator = function (a, b) {
            ok(a != null && b != null, 'key is null')
            return a < b ? -1 : a > b ? 1 : 0
        }
    }
    return new Strata(options)
}

var invoke = cadence(function (async, tmp, okay, test) {
    async(function () {
        rimraf(tmp, async())
    }, function () {
        fs.mkdir(tmp, 0755, async())
    }, function () {
        okay.global = function (name, value) {
            global[name] = value
            okay.leak(name)
        }
        okay.global('createStrata', createStrata)
        okay.global('tmp', tmp)
        okay.global('load', load)
        okay.global('stringify', stringify)
        okay.global('serialize', serialize)
        okay.global('gather', gather)
        okay.global('vivify', vivify)
        okay.global('script', script)
        test(okay, async())
    }, function () {
        if (!('UNTIDY' in process.env)) {
            rimraf(tmp, async())
        }
    })
})

module.exports = function (module, dirname) {
    var tmp = dirname + '/tmp'
    module.exports = function (count, test) {
        require('proof')(count, cadence(function (async, okay) {
            okay.global = function (name, value) {
                global[name] = value
                okay.leak(name)
            }
            okay.global('createStrata', createStrata)
            okay.global('tmp', tmp)
            okay.global('load', load)
            okay.global('stringify', stringify)
            okay.global('serialize', serialize)
            okay.global('gather', gather)
            okay.global('vivify', vivify)
            okay.global('script', script)
            async(function () {
                rimraf(tmp, async())
            }, function () {
                fs.mkdir(tmp, 0755, async())
            }, function () {
                cadence(test)(okay, {
                    createStrata: createStrata,
                    tmp: tmp,
                    load: load,
                    stringify: stringify,
                    serialize: serialize,
                    gather: gather,
                    vivify: vivify,
                    script: script,
                    tidy: cadence(function (async) {
                        rimraf(tmp, async())
                    })
                }, async())
            }, function () {
                if (!('UNTIDY' in process.env)) {
                    rimraf(tmp, async())
                }
            })
        }))
    }
}

function pretty (json) {
        function s (o) { return JSON.stringify(o) }
        function array (a) {
            return '[ ' + a.join(', ') + ' ]'
        }
        function obj (o) {
            var entries = []
            for (var k in o) {
                entries.push(s(k) + ': ' + s(o[k]))
            }
            return '{ ' + entries.join(', ') + ' }'
        }
        var buffer = []
        function puts (string) { buffer.push.apply(buffer, arguments) }
        puts('{\n')
        var fileSep = ''
        for (var file in json) {
            puts(fileSep, '    ', s(file), ': {\n')
            if (file % 2) {
                puts('        "log": [\n')
                var logSep = ''
                json[file].log.forEach(function (entry) {
                    puts(logSep, '            ', obj(entry))
                    logSep = ',\n'
                })
                puts('\n        ]')
                if (json[file].right) {
                    puts(',\n        "right": ' + json[file].right + '\n')
                } else {
                    puts('\n')
                }
            } else {
                puts('        "children": ', array(json[file].children), '\n')
            }
            puts('    }')
            fileSep = ',\n'
        }
        puts('\n}\n')
        return buffer.join('')
}

function script (options, callback) {
    var strata = new Strata({ directory: options.directory, branchSize: 3, leafSize: 3 })
    var queue = [{ type: 'create' }]

    var actions = {}

    actions.create = cadence(function (async, action) {
        async(function () {
            fs.readdir(options.directory, async())
        }, function (list) {
            list = list.filter(function (file) { return ! /^\./.test(file) })
            if (!list.every(function (file) { return /^\d+$/.test(file) })) {
                throw new Error('doesn\'t look like a strata directory')
            }
            async.forEach([ list ], function (file) { fs.unlink(file, async()) })
        }, function () {
            strata.create(async())
        })
    })

    var alphabet = 'abcdefghiklmnopqrstuvwxyz'.split('')

    function inc (string) {
        var parts = string.split('').reverse(), i = 0
        for (;;) {
            var letter = i < parts.length ? alphabet.indexOf(parts[i]) + 1 : 0
            if (letter == alphabet.length) letter = 0
            parts[i] = alphabet[letter]
            if (letter || ++i == parts.length) break
        }
        if (!letter) {
            parts.push('a')
        }
        return parts.reverse().join('')
    }

    actions.add = cadence(function (async, action) {
        async(function () {
            strata.mutator(action.values[0], async())
        }, function (cursor) {
            async.loop([], function () {
                cursor.indexOf(action.values[0], cursor.ghosts)
            }, function (index) {
                ok(index < 0)
                cursor.insert(action.values[0], action.values[0], ~index)
                action.values.shift()
                if (!action.values.length) {
                    async(function () {
                        cursor.unlock(async())
                    }, function () {
                        return [ async.break ]
                    })
                }
            })
        })
    })

    actions.remove = cadence(function (async, action) {
        var mutate, next
        async.loop([], function () {
            if (action.values.length) strata.mutator(action.values[0], async())
            else return [ async.break ]
        }, function (cursor) {
            action.values.shift()
            async(function () {
                if (cursor.index >= 0) cursor.remove(cursor.index)
            }, function () {
                cursor.unlock(async())
            })
        })
    })

    actions.balance = function (action, callback) {
        strata.balance(callback)
    }

    function print (tree, address, index, depth) {
        tree.forEach(function (child, index) {
            var padding = new Array(depth + 1).join('   ')
            if (child.address % 2) {
                var key = index ? child.children[0] : '<'
                while (key.length != 2) key = key + ' '
                process.stdout.write(padding + key + ' -> ')
                process.stdout.write(child.children.slice(child.ghosts).join(', ') +  '\n')
            } else {
                if (!('key' in child)) {
                    process.stdout.write(padding + '<\n')
                } else {
                    process.stdout.write(padding + child.key + '\n')
                }
                print(child.children, child.address, 0, depth + 1)
            }
        })
    }

    actions.vivify = cadence(function (async, action) {
        async(function () {
            strata.vivify(async())
        }, function (tree) {
            print(tree, 0, 0, 0)
        })
    })

    actions.stringify = cadence(function (async, action) {
        async(function () {
            stringify(options.directory, async())
        }, function (result) {
            fs.writeFile(action.file, pretty(JSON.parse(result)), 'utf8', async())
        })
    })

    actions.serialize = cadence(function (async, action) {
        async(function () {
            serialize(action.file, options.directory, async())
        }, function () {
            strata.open(async())
        })
    })

    actions.fixture = cadence(function (async, action) {
        async(function () {
            vivify(options.directory, async())
            load(action.file, async())
        }, function (actual, expected) {
            options.okay(actual, expected, action.file)
        })
    })

    function consume (callback) {
        if (queue.length) {
            var action = queue.shift()
            actions[action.type](action, function (error) {
                if (error) callback(error)
                else setImmediate(function () {
                    consume(callback)
                })
            })
        } else {
            callback()
        }
    }

    cadence(function (async) {
        var buffer = ''
        var fs = require('fs')
        async(function () {
            fs.readFile(options.file, 'utf8', async())
        }, function (body) {
            var lines = body.split(/\n/)
            lines.pop()
            lines.forEach(function (line) {
                switch (line[0]) {
                case '-':
                case '+':
                    var $ = /^[+-]([a-z]+)(?:-([a-z]+))?\s*$/.exec(line), values = []
                    values.push($[1])
                    $[2] = $[2] || $[1]
                    while ($[1] != $[2]) {
                        $[1] = inc($[1])
                        values.push($[1])
                    }
                    queue.push({ type: line[0] == '+' ? 'add' : 'remove', values: values })
                    break
                case '>':
                    queue.push({ type: 'stringify', file: line.substring(1) })
                    break
                case '<':
                    queue.shift()
                    queue.push({ type: 'serialize', file: line.substring(1) })
                    break
                case '=':
                    queue.push({ type: 'fixture', file: line.substring(1) })
                    break
                case '~':
                    queue.push({ type: 'balance' })
                    break
                case '!':
                    queue.push({ type: 'vivify' })
                    break
                }
            })
            async.forEach([ queue ], function (action) {
                actions[action.type](action, async())
            }, function () {
                setImmediate(async())
            })
        })
    })(callback)
}

module.exports.stringify = stringify
module.exports.serialize = serialize
module.exports.script = script
