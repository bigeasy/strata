#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 2, (Strata, tmp,  serialize, load, objectify, _) ->
  serialize "#{__dirname}/fixtures/split.before.json", tmp, _

  strata = new Strata tmp, leafSize: 3, branchSize: 3
  strata.open _

  records = []
  cursor = strata.iterator "a", _
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "c", "d" ], "records"

  cursor = strata.mutator "a", _
  cursor.remove cursor.indexOf("c", _), _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "d" ], "deleted"
