#!/usr/bin/env _coffee
fs = require "fs"
require("./harness") 4, ({ Strata, directory, fixture: { serialize } }, _) ->
  serialize "#{__dirname}/fixtures/one.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  cursor = strata.iterator("a", _)
  @ok cursor.found, "found"
  @equal cursor.index, 0, "found"
  @equal cursor.length, 1, "length"

  records = []
  for i in [cursor.index...cursor.length]
    records.push cursor.get(i, _)
  cursor.unlock()

  @deepEqual records, [ "a" ], "records"
