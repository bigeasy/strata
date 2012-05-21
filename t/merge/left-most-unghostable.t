#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 4, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/merge.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  cursor = strata.mutator "a", _
  cursor.delete cursor.index, _
  cursor.unlock()

  @equal cursor.index, 0, "unghostable"

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "b", "c", "d" ], "records"

  strata.balance _

  records = []
  cursor = strata.iterator "a", _
  @say cursor.offset
  @say cursor._page.ghosts
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "b", "c", "d" ], "merged"

  expected = load "#{__dirname}/fixtures/left-most-unghostable.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "after"
