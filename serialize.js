var fs = require('fs')
var util = require('util')

var json = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'))

var directory = {}

var checksum = 40

function addressify (address) {
    return address % 2 ? - address : address
}

for (var address in json) {
    var object = json[address]
    if (object.children) {
        directory[address] = [ object.children.map(addressify) ]
    } else {
        var ghosts = 0
        var positions = []
        var position = 0
        var bookmark = 0
        var order = []
        var records = 0
        directory[address] = object.log.map(function (entry, count) {
            var record, index
            switch (entry.type) {
            case 'pos':
                bookmark = position
                record = [ 0, 1, addressify(object.right || 0), ghosts, count + 1, positions.slice(), bookmark ]
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
                record = [ index + 1, records, count + 1, entry.value, bookmark ]
                break
            case 'del':
                records--;
                record = [ -(entry.index + 1), records, count + 1, bookmark ]
            }
            position += JSON.stringify(record).length + 1 + checksum + 1
            return record
        })
    }
}

console.log(util.inspect(directory, false, null))

/* vim: set sw=2 ts=2: */
