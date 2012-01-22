#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 2, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/split.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  cassette = strata.cassette("b")
  cursor = strata.cursor cassette, _
  index = cursor.insert cassette, _
  cursor.unlock()

  records = []
  cursor = strata.cursor "a", _
  for i in [cursor.index...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d" ]

  strata.balance _

  @ok 1, "two"
