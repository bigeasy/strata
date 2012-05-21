#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 5, ({ Strata, directory, fixture: { serialize, load, objectify } }, _) ->
  serialize "#{__dirname}/fixtures/delete.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d" ], "records"

  cursor = strata.mutator "c", _
  cursor.delete cursor.indexOf("c", _), _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d" ], "deleted"

  expected = load "#{__dirname}/fixtures/ghost.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "directory"

  strata.close _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  records = []
  cursor = strata.iterator "a", _
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.next(_)
  @equal cursor.offset, 1, "ghosted"
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d" ], "reopened"

  strata.close _
