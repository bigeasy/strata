var fs = require('fs');
var util = require('util');

var json = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

var copy = {};
for (file in json) {
  copy[file.replace(/^segment0*/, '') || 0] = json[file];
}
json = copy;

var leaves = {}, leaf = 1;
while (leaf) {
  leaves[leaf] = true;
  leaf = Math.abs(json[leaf].filter(function (line) { return ! line[0] }).pop()[2]);
}

var addresses = Object.keys(json)
                      .map(function (address) { return + address })
                      .sort(function (a, b) { return +(a) - +(b) });

var next = 0;
var map = {};
addresses.forEach(function (address) {
  if (leaves[address]) {
    while (!(next % 2)) next++;
  } else {
    while (next % 2) next++;
  }
  map[address] = next++;
})

var copy = {}
for (var file in json)  {
  var body = json[file];
  if (map[file] % 2) {
    body.filter(function (line) {
      return !line[0];
    }).forEach(function (line) {
      if (line[2]) line[2] = map[Math.abs(line[2])]
    });
  } else {
    body[0] = body[0].map(function (address) { return map[Math.abs(address)] });
  }
  copy[map[file]] = json[file];
}
json = copy

dir = json
var output = {};
for (var file in dir) {
  var record;
  if (file % 2) {
    record = { log: [] };
    dir[file].forEach(function (line) {
      if (line[0] == 0) {
        if (line[2]) record.right = Math.abs(line[2]);
        record.log.push({ type: 'pos' });
      } else if (line[0] > 0) {
        record.log.push({ type: 'add', value: line[3] });
      } else {
        record.log.push({ type: 'del', index: Math.abs(line[0]) - 1 });
      }
    })
  } else {
    record = { children: dir[file][0].map(function (address) { return Math.abs(address) }) };
  }
  output[file] = record;
}
json = output;

var __slice = [].slice;
function s (o) { return JSON.stringify(o) }
function puts (s) {
  process.stdout.write(__slice.call(arguments).join(''))
}
function array (a) {
  return '[ ' + a.join(', ') + ' ]';
}
function obj (o) {
  var entries = [];
  for (var k in o) {
    entries.push(s(k) + ': ' + s(o[k]));
  }
  return '{ ' + entries.join(', ') + ' }';
}

puts('{\n');
var fileSep = ''
for (var file in json) {
  puts(fileSep, '    ', s(file), ': {\n');
  if (file % 2) {
    puts('        "log": [\n');
    var logSep = '';
    json[file].log.forEach(function (entry) {
      puts(logSep, '            ', obj(entry));
      logSep = ',\n';
    });
    puts('\n        ]');
    if (json[file].right) {
      puts(',\n        "right": ' + json[file].right + '\n');
    } else {
      puts('\n');
    }
  } else {
    puts('        "children": ', array(json[file].children), '\n');
  }
  puts('    }');
  fileSep = ',\n'
}
puts('\n}\n');

/* vim: set sw=2 ts=2: */
