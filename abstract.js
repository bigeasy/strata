var dir = require('./serialized');

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
      }
    })
  } else {
    record = { children: dir[file][0].map(function (address) { return Math.abs(address) }) };
  }
  output[file] = record;
}

console.log(require('util').inspect(output, false, null));

/* vim: set sw=2 ts=2: */
