var fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , Strata = require("..")
  ;

function check (callback, forward) {
  return function (error, result) {
    if (error) callback(error);
    else forward(result);
  }
}

function objectify (directory, callback) {
  var files, dir = {}, count = 0;

  fs.readdir(directory, check(callback, list));

  function list ($1) {
    (files = $1).forEach(function (file) {
      if (!/^\./.test(file)) readFile(file);
      else read();
    });
  }

  function readFile (file) {
    dir[file] = [];

    fs.readFile(path.resolve(directory, file), "utf8", check(callback, lines));

    function lines (lines) {
      lines = lines.split(/\n/);
      lines.pop();
      lines.forEach(function (json, index) {
        json = json.replace(/[\da-f]+$/, "");
        dir[file].push(JSON.parse(json));
      });
      read();
    }
  }

  function read () {
    if (++count == files.length) callback(null, renumber(order(abstracted(dir))));
  }
}

function stringify (directory, callback) {
  objectify(directory, check(callback, segments));

  function segments (segments) {
    callback(null, JSON.stringify(segments, null, 2));
  }
}

function load (segments, callback) {
  fs.readFile(segments, "utf8", check(callback, parse));

  function parse (json) {
    callback(null, renumber(order(JSON.parse(json))));
  }
}

function insert (step, strata, values) {
  step(function () {
    values.sort();
    strata.mutator(values[0], step());
  }, function (cursor) {
    step(function () {
      cursor.insert(values[0], values[0], ~ cursor.index, step());
    }, function () {
      cursor.unlock();
    });
  });
}

function gather (step, strata) {
  var records = [], page, item;
  step(function () {
    records = []
    strata.iterator(step());
  }, function (cursor) {
    step(function () {
      return true;
    }, page = function (more) {
      if (more) return cursor.offset;
      cursor.unlock();
      step(null, records);
    }, item = function (i) {
      if (i < cursor.length) {
        cursor.get(i, step());
        step()(null, i);
      } else {
        step.jump(page);
        cursor.next(step());
      }
    }, function (record, i) {
      records.push(record);
      step.jump(item);
      step()(null, i + 1);
    });
  });
}

function serialize (segments, directory, callback) {
  if (typeof segments == "string") load(segments, check(callback, write));
  else write (segments);

  function write (json) {
    var dir = createDirectory(json);
    var files = Object.keys(dir);
    var count = 0;

    files.forEach(function (file) {
      var records = [];
      dir[file].forEach(function (line) {
        var record = [ JSON.stringify(line) ];
        record.push(crypto.createHash("sha1").update(record[0]).digest("hex"));
        record = record.join(" ");
        records.push(record);
      });
      records = records.join("\n") + "\n";
      fs.writeFile(path.resolve(directory, String(file)), records, "utf8", check(callback, written));
    });

    function written () { if (++count == files.length) callback(null) }
  }
}

function abstracted (dir) {
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

  return output;
}

function renumber (json) {
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

  return copy;
}

function order (json) {
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
        }
      })
      object.order = order;
    }
  }
  return json;
}

function createDirectory (json) {
  var directory = {};

  var checksum = 40;

  function addressify (address) {
    return address % 2 ? - address : address;
  }

  for (var address in json) {
    var object = json[address];
    if (object.children) {
      directory[address] = [ object.children.map(addressify) ];
    } else {
      var ghosts = 0;
      var positions = [];
      var position = 0;
      var order = [];
      var records = 0;
      directory[address] = object.log.map(function (entry, count) {
        var record;
        var index;
        switch (entry.type) {
        case 'pos':
          record = [ 0, 1, addressify(object.right || 0), ghosts, count + 1, positions.slice() ];
          break;
        case 'add':
          records++;
          for (index = 0; index < order.length; index++) {
            if (order[index] > entry.value) {
              break;
            }
          }
          order.splice(index, 0, entry.value);
          positions.splice(index, 0, position);
          record = [ index + 1, records, count + 1, entry.value  ];
          break;
        }
        position += JSON.stringify(record).length + 1 + checksum + 1;
        return record;
      })
    }
  }

  return directory;
}

function deltree (directory, callback) {
  var files, count = 0;

  readdir();

  function readdir () {
    fs.readdir(directory, extant);
  }

  function extant (error, $1) {
    if (error) {
      if (error.code != "ENOENT") callback(error);
      else callback();
    } else {
      list($1);
    }
  }

  function list ($1) {
    (files = $1).forEach(function (file) {
      stat(path.resolve(directory, file));
    });
    deleted();
  }

  function stat (file) {
    var stat;

    fs.stat(file, check(callback, inspect));

    function inspect ($1) {
      if ((stat = $1).isDirectory()) deltree(file, check(callback, unlink));
      else unlink();
    }

    function unlink () {
      if (stat.isDirectory()) fs.rmdir(file, check(callback, deleted));
      else fs.unlink(file, check(callback, deleted));
    }
  }

  function deleted () {
    if (++count > files.length) fs.rmdir(directory, callback);
  }
}

module.exports = function (dirname) {
  var tmp = dirname + "/tmp";
  return require("proof")(function (step) {
    deltree(tmp, step());
  }, function (step) {
    step(function () {
      fs.mkdir(tmp, 0755, step());
    }, function () {
      return { Strata: Strata
             , tmp: tmp
             , load: load
             , stringify: stringify
             , insert: insert
             , serialize: serialize
             , gather: gather
             , objectify: objectify
             };
    });
  });
};

module.exports.stringify = stringify;

/* vim: set sw=2 ts=2: */
