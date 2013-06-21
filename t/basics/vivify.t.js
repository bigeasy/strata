#!/usr/bin/env node

require("./proof")(1, function (Strata, tmp, serialize, deepEqual, step, gather, say) {
  var fs = require ('fs'), strata;
  step(function () {
    serialize(__dirname + "/fixtures/vivify.json", tmp, step());
  }, function () {
    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.vivify(step());
  }, function (result) {
    deepEqual(result,
      [ { address: 12,
          children:
           [ { address: -1, children: [ 'a', 'b' ] },
             { address: -9, children: [ 'c', 'd', 'e' ] },
             { address: -8, children: [ 'f', 'g', 'h' ] } ] },
        { address: 11,
          children:
           [ { address: -7, children: [ 'i', 'j', 'k' ] },
             { address: -6, children: [ 'l', 'm', 'n' ] },
             { address: -5, children: [ 'o', 'p', 'q' ] } ],
          key: 'l' },
        { address: 10,
          children:
           [ { address: -4, children: [ 'r', 's', 't' ] },
             { address: -3, children: [ 'u', 'v', 'w' ] },
             { address: -2, children: [ 'x', 'y', 'z' ] } ],
          key: 'x' } ], 'vivify');
  });
});
