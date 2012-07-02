#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 2, (Strata, tmp, load, objectify, serialize, _) ->
  serialize "#{__dirname}/fixtures/split.before.json", tmp, _

  strata = new Strata tmp, leafSize: 3, branchSize: 3
  strata.open _

  cursor = strata.mutator "b", _
  cursor.insert "b", "b", ~ cursor.index, _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d" ], "records"

  strata.balance _

  expected = load "#{__dirname}/fixtures/split.after.json", _
  actual = objectify tmp, _

  @say expected
  @say actual

  @deepEqual actual, expected, "split"
