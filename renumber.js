var fs = require('fs');
var util = require('util');

var json = JSON.parse(fs.readFileSync('t/basics/fixtures/merge.before.json', 'utf8'));

var directory = {};

var checksum = 40;

var addresses = Object.keys(json)
                      .map(function (address) { return + address })
                      .sort(function (a, b) { return +(a) - +(b) });

var next = 0;
var map = {};
addresses.forEach(function (address) {
  while ((address % 2) != (next % 2)) next++;
  map[address] = next++;
})

var copy = {}
for (var address in json)  {
  var object = json[address];
  if (address % 2) {
    object.right && (object.right = map[object.right]);
  } else {
    object.children = object.children.map(function (address) {
      return map[address];
    })
  }
  copy[map[address]] = json[address];
}

console.log(require('util').inspect(copy, false, null));

/* vim: set sw=2 ts=2: */
