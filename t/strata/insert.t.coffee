#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 2, ({ Strata, directory, fixture: { load, objectify } }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3

  strata.create _

  cassette = strata.cassette("a")
  console.error { cassette }
  cursor = strata.cursor cassette, _

  cursor.insert cassette, _

  cursor.unlock()

  @equal strata._io.size, 3, "json size"

  strata.close _

  expected = load "#{__dirname}/fixtures/insert.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "insert"
