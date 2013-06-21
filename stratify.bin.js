#!/usr/bin/env node

/*

  ___ usage: en_US ___
  stratify [options]

  options:

  -d, --directory         [name]  Name of directory to store database.

  ___ usage ___

 */

var Strata = require('./index'), processing = false, queue = [ { type: 'create' } ];

var cadence = require('cadence');


require('arguable').parse(__filename, process.argv.slice(2), function (options) {
  var strata = new Strata(options.params.directory, { branchSize: 3, leafSize: 3 });

  var actions = {};

  actions.create = function (action, callback) {
    strata.create(callback);
  }

  actions.add = cadence(function (step, action) {
    step(function () {
      strata.mutator(action.value, step());
    }, function (cursor) {
      step(function () {
        // If index is positive; problem.
        cursor.insert(action.value, action.value, ~ cursor.index, step());
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
        console.log(padding + (index ? child.children[0] : '<'));
        child.children.forEach(function (value) {
          process.stdout.write('   ' + padding + value +  '\n');
        });
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

  actions.stringify = cadence(function (step) {
    console.log('stringify');
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
        case '+':
          queue.push({ type: 'add', value: line.substring(1) });
          break;
        case '>':
          queue.push({ type: 'stringify' });
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
