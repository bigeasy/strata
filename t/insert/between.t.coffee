#!/usr/bin/env _coffee
fs = require "fs"
require("./harness") 3, ({ Strata, directory, fixture: { serialize, load, objectify } }, _) ->
  serialize "#{__dirname}/fixtures/between.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  cassette = strata.cassette("b")
  cursor = strata.mutator(cassette.key, _)
  @say { index: cursor.index, cassette }
  cursor.insert(cassette.record, cassette.key, ~ cursor.index,  _)
  cursor.unlock()

  expected = load "#{__dirname}/fixtures/between.after.json", _
  actual = objectify directory, _

  @deepEqual actual, expected, "insert"

  positions = strata._io.cache[-1].positions.slice(0)

  strata.close(_)

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  records = []
  cursor = strata.iterator("a", _)
  for i in [cursor.offset...cursor.length]
    records.push cursor.get(i, _)
  cursor.unlock()

  @deepEqual strata._io.cache[-1].positions, positions, "reload"

  @deepEqual records, [ "a", "b", "c" ], "records"
