var fs = require('fs');
var util = require('util');

var json = JSON.parse(fs.readFileSync('empty.before.json', 'utf8'));

for (var address in json) {
  var object = json[address];
  if (address % 2) {
    var order = [];
    object.log.forEach(function (entry) {
      var index;
      switch (entry.type) {
      case 'add':
        for (index = 0; index < order.length; index++) {
          if (order[index] > entry.value) {
            break;
          }
        }
        order.splice(index, 0, entry.value);
        break;
      case 'del':
        order.splice(entry.index, 1);
        break;
      }
    })
    object.order = order;
  }
}

console.log(util.inspect(json, false, null));

/* vim: set sw=2 ts=2: */
