#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 3, ({ Strata, directory, fixture: { serialize, load, objectify } }, _) ->
  serialize "#{__dirname}/fixtures/between.after.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  cassette = strata.cassette("b")
  cursor = strata.cursor(cassette, _)
  cursor.insert(cassette, _)
  cursor.unlock()

  expected = load "#{__dirname}/fixtures/between.after.json", _
  actual = objectify directory, _

  @deepEqual actual, expected, "insert"

  positions = strata._io.cache[-1].positions.slice(0)

  strata.close(_)

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  records = []
  cursor = strata.cursor("a", _)
  for i in [cursor.index...cursor.length]
    records.push cursor.get(i, _)
  cursor.unlock()

  @deepEqual strata._io.cache[-1].positions, positions, "reload"

  @deepEqual records, [ "a", "b", "c" ], "records"
