var sys = require('sys'),
    fs = require('fs'),
    Buffer = require('buffer').Buffer;

// So, how do this efficent properties work? Surely, if I create an object and
// use it as map, it must revert to a hashed implementation. How do I get it to
// treat it as class versus a map? Am I defeating V8's genius in some way? 


function extend (a, b) {
  for (var key in b) a[key] = b[key];
  return a;
}


function instance(options) {
  var queue = [], readers = [], reading = 0, root;

  queue.push(readers);

  storage = require('./internal/storage').instance(options.directory);

  /*
  function read(position, callback) {
  }

  var leaf = {
      add: function (position, value, operations, callback) {
      }
  };

  var inner = {
      offset: function (index) {
          return 8 + (options.io.size + 8) * index;
      },
      descend: function (buffer, index, value, callback) {
          var position = buffer.unpack(innerOffset(index) + options.io.size, 'foo');
          if (inner.isLeaf(buffer)) {
              leaf.find(position, value, callback);
          } else {
              inner.get(position, value, callback);
          }
      },
      search: function(tier, value, low, high, mid, callback) {
          if (low < high) {
              callback(buffer, mid);
          } else {
              var mid = low + ((high - low) / 2);
              tier.get(mid, function (record) {
                  int compare = options.io.sort(value, record); 
                  if (compare < 0) {
                      inner.search(buffer, value, low, mid - 1, mid, callback);
                  } else if (compare > 0) {
                      inner.search(buffer, value, mid + 1, high, mid, callback);
                  }
              });
          }
      },
      get: function (position, value, callback) {
          inner.find(position, value, fuction (buffer, index) {
              buffer.descend(buffer, index, value, callback);
          });
      },
      find: function (position, value, callback) {
          inner.read(position, function (tier) {
              inner.search(tier, value, 0, innerSize(buffer), 0, callback);
          });
      },
      add: function (position, value, operations, callback) {
          inner.find(position, value, function (tier, index, record) {
              if (options.io.innerSize == tier.size) {
                  operations.push(function () {
                      inner.split(position);
                  });
              } else {
                  operations.clear();
              }
              if (tier.hasLeaves) {
                  leaf.add(position, value, operations, callback);
              } else {
                  inner.add(position, value, operations, callback);
              }
          });
      },
      split: function (position, callback) {
      },
      remove: function (position, value, callback) {
      }
  };


  function add(position, value, callback) {
      read(position, function (buffer) {
          var operations = [];
          int size = inner.count(buffer);
          if (size == options.io.innerSize) {
              operations.push(root.split);
          }
          inner.add(root, value, operations, callback);
      });
  }

  return {
      get: function (value, callback) {
          readers.push(function () {
              inner.get(root, value, callback);
          });
      },
      add: function (value, callback) {
          mutate(function () {
              inner.add(root, value, callback);
          });
      },
      remove: function (value, callback) {
          mutate(function () {
              inner.remove(root, value, callback);
          });
      }
  } */

  var root = {
      position: 0,
      /*
      split: function (callback) {
          var left = inner.create();
          var right = inner.create();
          var i = 0;
          for (; i < options.io.innerSize / 2; i++) {
              left.add(root.tier.get(i));
          }
          for (; i < options.io.innerSize; i++) {
              right.add(root.tier.get(i));
          }
          root.tier.clear();
          root.tier.add({ record: null, position: left.position });
          root.tier.add({ record: right.get(0), position: right.position });
          left.write(root.tier.get(0).position, function () {
              right.write(root.tier.get(1).position, function () {
                  root.tier.write(root.position, callback);
              });
          });
      },
      */
      create: function () {
      },
      merge: function() {
      }
  };

  function enqueue() {
    if (queue.length % 2 == 1) {
      while (queue[0].length != 0) {
        var reader = queue[0].shift();
        reading++;
        reader(function () {
          reading--;
          enqeue();
        });
      } 
      if (queue.length > 1 && reading == 0) {
        queue.shift();
        queue[0](function () {
          queue.shift();
          enqueue();
        });
      }
    }
  }

  function mutate (writer) {
    readers = [];

    queue.push(writer);
    queue.push(readers);

    enqueue();
  }

  return {
    file: options.file,
    make: function () {
      fd = fs.openSync(options.file, 'a+');
      mutate(function (callback) {
        root.tier = storage.branch.allocate(true);
        root.tier.add(0, storage.leaf.allocate(), callback);
      });
    },
    add: function (value) {
    }
  }
}

module.exports = { instance: instance, serializer: serializer };

/* vim: set ts=2 sw=2 et tw=0: */
