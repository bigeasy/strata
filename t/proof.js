var fs = require('fs'),
    path = require('path'),
    crypto = require('crypto'),
    Strata = require('..'),
    ok = require('assert').ok

function check (callback, forward) {
    return function (error, result) {
        if (error) callback(error)
        else forward(result)
    }
}

function objectify (directory, callback) {
    var files, dir = {}, lengths = {}, count = 0

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

// todo: pretty print should be in here, so I can use it from stratify and the
// stringify utility.
function stringify (directory, callback) {
    objectify(directory, check(callback, segments))

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

function insert (step, strata, values) {
    step(function () {
        values.sort()
        strata.mutator(values[0], step())
    }, function (cursor) {
        step(function () {
            cursor.insert(values[0], values[0], ~ cursor.index, step())
        }, function () {
            cursor.unlock()
        })
    })
}

function gather (step, strata) {
    var records = [], page, item
    step(function () {
        records = []
        strata.iterator(strata.left, step())
    }, function (cursor) {
        step(function (more) {
            if (!more) {
                cursor.unlock()
                step(null, records)
            } else {
                step(function () {
                    step(function (index) {
                        step(function () {
                            cursor.get(index + cursor.offset, step())
                        }, function (record) {
                            records.push(record)
                        })
                    })(cursor.length - cursor.offset)
                }, function () {
                    cursor.next(step())
                })
            }
        })(null, true)
    })
}

function serialize (segments, directory, callback) {
    if (typeof segments == 'string') load(segments, check(callback, write))
    else write (segments)

    function write (json) {
        var dir = directivize(json)
        var files = Object.keys(dir)
        var count = 0

        files.forEach(function (file) {
            var records = []
            dir[file].forEach(function (line) {
                var record = [ JSON.stringify(line.header) ]
                var hash = crypto.createHash('sha1')
                hash.update(record[0])
                if (line.body) {
                    var body = JSON.stringify(line.body)
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
            fs.writeFile(path.resolve(directory, String(file)), records, 'utf8', check(callback, written))
        })

        function written () { if (++count == files.length) callback(null) }
    }
}

function abstracted (dir, lengths) {
    var output = {}
    var position = 0
    var bookmark

    for (var file in dir) {
        var record
        if (file % 2) {
            record = { log: [] }
            position = 0
            dir[file].forEach(function (line, index) {
                var json = line.header
                if (json[0]) {
                    ok(index + 1 == json[0], 'entry record is wrong')
                    var length = lengths[file][index]
                    if (json[1] == 0) {
                        bookmark = { position: position, length: length, entry: index + 1 }
                        record.log.push({ type: 'pos' })
                    } else if (json[1] > 0) {
                        record.log.push({ type: 'add', value: line.body })
                    } else {
                        record.log.push({ type: 'del', index: Math.abs(json[1]) - 1 })
                    }
                    position += length
                } else {
                    ok(index == dir[file].length - 1, 'footer not last entry')
                    if (json[4]) record.right = Math.abs(json[4])
                    if (json[1] != bookmark.position || json[2] != bookmark.length || json[3] != bookmark.entry) {
                        console.log(require('util').inspect(dir, false, null), json, bookmark)
                        throw new Error
                    }
                }
            })
        } else {
            var children = []
            dir[file].forEach(function (json, index) {
                if (json.header[1] > 0) {
                    children.splice(json.header[1] - 1, 0, json.header[2])
                } else {
                    children.splice(~json.header[1], 1)
                }
            })
            record = { children: children }
        }
        output[file] = record
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
            var bookmark
            directory[address] = object.log.map(function (entry, count) {
                var record
                var index
                switch (entry.type) {
                case 'pos':
                    record = [ count + 1, 0, ghosts ]
                    record = { header: record.concat(positions).concat(lengths) }
                    break
                case 'add':
                    records++
                    for (index = 0; index < order.length; index++) {
                        if (order[index] > entry.value) {
                            break
                        }
                    }
                    order.splice(index, 0, entry.value)
                    positions.splice(index, 0, position)
                    record = { header: [ count + 1, index + 1 ], body: entry.value }
                    break
                case 'del':
                    records--
                    record = { header: [ count + 1, -(entry.index + 1) ] }
                    break
                }
                var length = JSON.stringify(record.header).length + 1 + checksum + 1
                if (record.body != null) {
                    length += JSON.stringify(record.body).length + 1
                }
                var entire = length + String(length).length + 1
                length = Math.max(entire, length + String(entire).length + 1)
                switch (entry.type) {
                case 'pos':
                    bookmark = { position: position, length: length, entry: count + 1 }
                    break
                case 'add':
                    lengths.splice(index, 0, length)
                    break
                }
                position += length
                return record
            })
            directory[address].push({ header: [
                0, bookmark.position, bookmark.length, bookmark.entry, object.right || 0, position, directory[address].length, ghosts, records
            ]})
        }
    }

    return directory
}

function deltree (directory, callback) {
    var files, count = 0

    readdir()

    function readdir () {
        fs.readdir(directory, extant)
    }

    function extant (error, $1) {
        if (error) {
            if (error.code != 'ENOENT') callback(error)
            else callback()
        } else {
            list($1)
        }
    }

    function list ($1) {
        (files = $1).forEach(function (file) {
            stat(path.resolve(directory, file))
        })
        deleted()
    }

    function stat (file) {
        var stat

        fs.stat(file, check(callback, inspect))

        function inspect ($1) {
            if ((stat = $1).isDirectory()) deltree(file, check(callback, unlink))
            else unlink()
        }

        function unlink () {
            if (stat.isDirectory()) fs.rmdir(file, check(callback, deleted))
            else fs.unlink(file, check(callback, deleted))
        }
    }

    function deleted () {
        if (++count > files.length) fs.rmdir(directory, callback)
    }
}

module.exports = function (dirname) {
    var tmp = dirname + '/tmp'
    return require('proof')(function (step) {
        deltree(tmp, step())
    }, function (step) {
        step(function () {
            fs.mkdir(tmp, 0755, step())
        }, function () {
            return {
                Strata: Strata,
                tmp: tmp,
                load: load,
                stringify: stringify,
                insert: insert,
                serialize: serialize,
                gather: gather,
                objectify: objectify,
                script: script
            }
        })
    })
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
    var cadence = options.cadence

    var actions = {}

    actions.create = cadence(function (step, action) {
        step(function () {
            fs.readdir(options.directory, step())
        }, function (list) {
            list = list.filter(function (file) { return ! /^\./.test(file) })
            if (!list.every(function (file) { return /^\d+$/.test(file) })) {
                throw new Error('doesn\'t look like a strata directory')
            }
            step(function (file) { fs.unlink(file, step()) })(list)
        }, function () {
            strata.create(step())
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

    actions.add = cadence(function (step, action) {
        step(function () {
            strata.mutator(action.values[0], step())
        }, function (cursor) {
            step(function () {
                cursor.indexOf(action.values[0], step())
            }, function (index) {
                ok(index < 0)
                cursor.insert(action.values[0], action.values[0], ~ index, step())
                action.values.shift()
            }, function () {
                if (!action.values.length) {
                        cursor.unlock()
                        step(null)
                }
            })()
        })
    })

    actions.remove = cadence(function (step, action) {
        var mutate, next
        step(function () {
            if (action.values.length) strata.mutator(action.values[0], step())
            else step(null)
        }, function (cursor) {
            action.values.shift()
            step(function () {
                if (cursor.index >= 0) cursor.remove(cursor.index, step())
            }, function () {
                cursor.unlock()
            })
        })()
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

    actions.vivify = cadence(function (step, action) {
        step(function () {
            strata.vivify(step())
        }, function (tree) {
            print(tree, 0, 0, 0)
        })
    })

    actions.stringify = cadence(function (step, action) {
        step(function () {
            stringify(options.directory, step())
        }, function (result) {
            fs.writeFile(action.file, pretty(JSON.parse(result)), 'utf8', step())
        })
    })

    actions.serialize = cadence(function (step, action) {
        step(function () {
            serialize(action.file, options.directory, step())
        }, function () {
            strata.open(step())
        })
    })

    actions.fixture = cadence(function (step, action) {
        step(function () {
            objectify(options.directory, step())
            load(action.file, step())
        }, function (actual, expected) {
            options.deepEqual(actual, expected, action.file)
        })
    })

    function consume (callback) {
        if (queue.length) {
            var action = queue.shift()
            actions[action.type](action, function (error) {
                if (error) callback(error)
                else process.nextTick(function () {
                    consume(callback)
                })
            })
        } else {
            callback()
        }
    }

    cadence(function (step) {
        var buffer = ''
        var fs = require('fs')
        step(function () {
            fs.readFile(options.file, 'utf8', step())
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
            step(function (action) {
                actions[action.type](action, step())
            }, function () {
                process.nextTick(step())
            })(queue)
        })
    })(callback)
}

module.exports.stringify = stringify
module.exports.serialize = serialize
module.exports.script = script
