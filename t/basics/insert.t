#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 3, ({ Strata, directory, fixture: { load, objectify } }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3

  strata.create _

  cassette = strata.cassette("a")
  cursor = strata.mutator "a", _

  inserted = cursor.insert cassette.record, cassette.key, ~ cursor.index, _

  @ok inserted, "inserted"
  cursor.unlock()

  @equal strata._io.size, 32, "json size"

  strata.close _

  expected = load "#{__dirname}/fixtures/insert.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "insert"

  @say expected.segment00000001
