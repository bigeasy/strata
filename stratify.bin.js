#!/usr/bin/env node

/*

  ___ usage: en_US ___
  stratify [options]

  options:

  -d, --directory         [name]  Name of directory to store database.

  ___ usage ___

 */

var Strata = require('./index'), processing = false, queue = [ { type: 'create' } ];

var cadence = require('cadence'), ok = require('assert'), fs = require('fs');

var stringify = require('./t/proof').stringify;

require('arguable').parse(__filename, process.argv.slice(2), function (options) {
  var strata = new Strata(options.params.directory, { branchSize: 3, leafSize: 3 });

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
      var next;
      step(next = function () {
        cursor.indexOf(action.values[0], step());
      }, function (index) {
        ok(index < 0);
        cursor.insert(action.values[0], action.values[0], ~ index, step());
        action.values.shift();
        if (action.values.length) step.jump(next);
      }, function () {
        cursor.unlock();
      });
    });
  });

  actions.remove = cadence(function (step, action) {
    var mutate, next;
    step(next = function () {
      if (action.values.length) strata.mutator(action.values[0], step());
      else step(null);
    }, function (cursor) {
      action.values.shift();
      step.jump(next);
      if (cursor.index >= 0) step(function () {
        cursor.remove(cursor.index, step());
      }, function () {
        cursor.unlock();
      });
    });
  });

  actions.balance = function (action, callback) {
    strata.balance(callback);
  }

  function print (tree, address, index, depth) {
    tree.forEach(function (child, index) {
      var padding = new Array(depth + 1).join('   ');
      if (child.address < 0) {
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
      stringify(options.params.directory, step());
    }, function (result) {
      console.log(action, result);
      fs.writeFile(action.file, result, 'utf8', step());
    });
  });

  function consume (callback) {
    if (queue.length) {
      processing = true;
      var action = queue.shift();
      actions[action.type](action, function (error) {
        if (error) callback(error);
        else process.nextTick(function () {
          consume(callback);
        });
      });
    } else {
      processing = false;
      callback();
    }
  }

  var buffer = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('readable', function () {
    var data;
    while ((data = process.stdin.read()) != null) {
      var lines = (buffer + data).split(/\n/);
      buffer = lines.pop();
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
        case '~':
          queue.push({ type: 'balance' });
          break;
        case '!':
          queue.push({ type: 'vivify' });
          break;
        }
      });
      if (!processing) consume(function (error) { if (error) throw error });
    }
  });

});
