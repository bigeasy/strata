#!/usr/bin/env node

require("./proof")(1, function (step, Strata, deepEqual, tmp, insert, load, objectify, equal, serialize) {

  function forward (name) { return function () { return fs[name].apply(fs, arguments) } }

  
  var fs = require('fs'), path = require('path'), proxy = {};
  for (var x in fs) {
    if (x[0] != '_') proxy[x] = forward(x);
  }
  proxy.unlink = function (file, callback) {
    var error = new Error();
    error.code = "EACCES";
    callback(error);
  }
  var strata = new Strata(tmp, { fs: proxy, leafSize: 3 });

  step(function () {
    serialize(__dirname + "/../basics/fixtures/split.before.json", tmp, step());
  }, function () {
    strata.open(step());
  }, function () {
    insert(step, strata, [ "b" ]);
  }, function () {
    strata.balance(step(Error));
  }, function (error) {
    equal(error.code, "EACCES", "unlink error");
  });
});
