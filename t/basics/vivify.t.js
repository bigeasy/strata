#!/usr/bin/env node

require('./proof')(1, function (Strata, tmp, serialize, deepEqual, step, gather, say) {
    var fs = require ('fs'), strata
    step(function () {
        serialize(__dirname + '/fixtures/vivify.json', tmp, step())
    }, function () {
        strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(step())
    }, function () {
        strata.vivify(step())
    }, function (result) {
        console.log(require('util').inspect(result, false, null))
        deepEqual(result,
          [ { address: 22,
              children:
               [ { address: 1, children: [ 'a', 'b' ], ghosts: 0 },
                 { address: 17, children: [ 'c', 'd', 'e' ], ghosts: 0 },
                 { address: 15, children: [ 'f', 'g', 'h' ], ghosts: 0 } ] },
            { address: 20,
              children:
               [ { address: 13, children: [ 'i', 'j', 'k' ], ghosts: 0 },
                 { address: 11, children: [ 'l', 'm', 'n' ], ghosts: 0 },
                 { address: 9, children: [ 'o', 'p', 'q' ], ghosts: 0 } ],
              key: 'i' },
            { address: 18,
              children:
               [ { address: 7, children: [ 'r', 's', 't' ], ghosts: 0 },
                 { address: 5, children: [ 'u', 'v', 'w' ], ghosts: 0 },
                 { address: 3, children: [ 'x', 'y', 'z' ], ghosts: 0 } ],
              key: 'r' } ], 'vivify')
    })
})
