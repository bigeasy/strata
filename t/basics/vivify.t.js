#!/usr/bin/env node

require("./proof")(1, function (Strata, tmp, serialize, deepEqual, step, gather, say) {
  var fs = require ('fs'), strata;
  step(function () {
    serialize(__dirname + "/fixtures/vivify.json", tmp, step());
  }, function () {
    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.vivify(step());
  }, function (result) {
    deepEqual(result,
      [ { address: 12,
          children:
           [ { address: -1, children: [ 'a', 'b' ], ghosts: 0 },
             { address: -9, children: [ 'c', 'd', 'e' ], ghosts: 0 },
             { address: -8, children: [ 'f', 'g', 'h' ], ghosts: 0 } ] },
        { address: 11,
          children:
           [ { address: -7, children: [ 'i', 'j', 'k' ], ghosts: 0 },
             { address: -6, children: [ 'l', 'm', 'n' ], ghosts: 0 },
             { address: -5, children: [ 'o', 'p', 'q' ], ghosts: 0 } ],
          key: 'i' },
        { address: 10,
          children:
           [ { address: -4, children: [ 'r', 's', 't' ], ghosts: 0 },
             { address: -3, children: [ 'u', 'v', 'w' ], ghosts: 0 },
             { address: -2, children: [ 'x', 'y', 'z' ], ghosts: 0 } ],
          key: 'r' } ], 'vivify');
  });
});
