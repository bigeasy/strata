#!/usr/bin/env node

/*

  ___ usage: en_US ___
  stratify [options]

  options:

  -d, --directory         [name]  Name of directory to store database.

  ___ usage ___

 */

var Strata = require('./index'), queue = [ { type: 'create' } ];

var cadence = require('cadence'), ok = require('assert'), fs = require('fs');

var harness = require('./t/proof');

function pretty (json) {
    function s (o) { return JSON.stringify(o) }
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
    var buffer = []
    function puts (string) { buffer.push.apply(buffer, arguments) }
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
    return buffer.join('')
}

require('arguable').parse(__filename, process.argv.slice(2), function (options) {
  script({
    directory: options.params.directory,
    file: options.argv.shift(),
    deepEqual: require('assert').deepEqual
  }, function (error) {
    if (error) throw error
  });
})

function script (options, callback) {
  var strata = new Strata({ directory: options.directory, branchSize: 3, leafSize: 3 });

  var actions = {};

  actions.create = function (action, callback) {
    strata.create(callback);
  }

  var alphabet = 'abcdefghiklmnopqrstuvwxyz'.split('');

  function inc (string) {
    var parts = string.split('').reverse(), i = 0;
    for (;;) {
      var letter = i < parts.length ? alphabet.indexOf(parts[i]) + 1 : 0;
      if (letter == alphabet.length) letter = 0;
      parts[i] = alphabet[letter];
      if (letter || ++i == parts.length) break;
    }
    if (!letter) {
      parts.push('a');
    }
    return parts.reverse().join('');
  }

  actions.add = cadence(function (step, action) {
    step(function () {
      strata.mutator(action.values[0], step());
    }, function (cursor) {
      step(function () {
        cursor.indexOf(action.values[0], step());
      }, function (index) {
        ok(index < 0);
        cursor.insert(action.values[0], action.values[0], ~ index, step());
        action.values.shift();
      }, function () {
        if (!action.values.length) {
            cursor.unlock();
            step(null);
        }
      })();
    });
  });

  actions.remove = cadence(function (step, action) {
    var mutate, next;
    step(function () {
      if (action.values.length) strata.mutator(action.values[0], step());
      else step(null);
    }, function (cursor) {
      action.values.shift();
      if (cursor.index >= 0) step(function () {
        cursor.remove(cursor.index, step());
      }, function () {
        cursor.unlock();
      });
    })();
  });

  actions.balance = function (action, callback) {
    strata.balance(callback);
  }

  function print (tree, address, index, depth) {
    tree.forEach(function (child, index) {
      var padding = new Array(depth + 1).join('   ');
      if (child.address % 2) {
        var key = index ? child.children[0] : '<';
        while (key.length != 2) key = key + ' ';
        process.stdout.write(padding + key + ' -> ');
        process.stdout.write(child.children.slice(child.ghosts).join(', ') +  '\n');
      } else {
        if (!('key' in child)) {
          process.stdout.write(padding + '<\n');
        } else {
          process.stdout.write(padding + child.key + '\n');
        }
        print(child.children, child.address, 0, depth + 1);
      }
    });
  }

  actions.vivify = cadence(function (step, action) {
    step(function () {
      strata.vivify(step());
    }, function (tree) {
      print(tree, 0, 0, 0);
    });
  });

  actions.stringify = cadence(function (step, action) {
    step(function () {
      harness.stringify(options.directory, step());
    }, function (result) {
      fs.writeFile(action.file, pretty(JSON.parse(result)), 'utf8', step());
    });
  });

  actions.serialize = cadence(function (step, action) {
    step(function () {
      harness.serialize(action.file, options.directory, step());
    }, function () {
      strata.open(step());
    });
  });

  function consume (callback) {
    if (queue.length) {
      var action = queue.shift();
      actions[action.type](action, function (error) {
        if (error) callback(error);
        else process.nextTick(function () {
          consume(callback);
        });
      });
    } else {
      callback();
    }
  }

  cadence(function (step) {
    var buffer = '';
    var fs = require('fs')
    step(function () {
      fs.readFile(options.file, 'utf8', step());
    }, function (body) {
      var lines = body.split(/\n/);
      lines.pop();
      lines.forEach(function (line) {
        switch (line[0]) {
        case '-':
        case '+':
          var $ = /^[+-]([a-z]+)(?:-([a-z]+))?\s*$/.exec(line), values = [];
          values.push($[1]);
          $[2] = $[2] || $[1];
          while ($[1] != $[2]) {
            $[1] = inc($[1]);
            values.push($[1]);
          }
          queue.push({ type: line[0] == '+' ? 'add' : 'remove', values: values });
          break;
        case '>':
          queue.push({ type: 'stringify', file: line.substring(1) });
          break;
        case '<':
          queue.shift();
          queue.push({ type: 'serialize', file: line.substring(1) });
          break;
        case '~':
          queue.push({ type: 'balance' });
          break;
        case '!':
          queue.push({ type: 'vivify' });
          break;
        }
      });
      step(function (action) {
        actions[action.type](action, step());
      }, function () {
        process.nextTick(step());
      })(queue);
    });
  })(callback);
}

/* vim: set ts=2 sw=2: */
