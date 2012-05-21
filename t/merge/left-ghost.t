#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 3, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/left-ghost.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  cursor = strata.mutator "d", _
  cursor.delete cursor.index, _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  loop
    console.log cursor._page.address, cursor.offset, cursor.length, cursor._page.length, cursor._page.positions.length
    for i in [cursor.offset...cursor.length]
      @say("i " + i)
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "e", "f", "g" ], "records"

  strata.balance _

  records = []
  cursor = strata.iterator "a", _
  loop
    console.log cursor._page.ghosts, cursor.offset, cursor.length, cursor._page.positions
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "e", "f", "g" ], "merged"

  expected = load "#{__dirname}/fixtures/left-ghost.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "after"
