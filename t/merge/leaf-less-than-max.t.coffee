#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 3, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/merge.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  cursor = strata.mutator "b", _
  cursor.delete cursor.index, _
  cursor.next _
  cursor.delete cursor.indexOf("d", _), _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.index...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "c" ], "records"

  strata.balance _

  expected = load "#{__dirname}/fixtures/leaf-less-than-max.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "merge"

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.index...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "c" ], "merged"
