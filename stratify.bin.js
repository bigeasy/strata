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
        }
      });
      if (!processing) consume(function (error) { if (error) throw error });
    }
  });

});
