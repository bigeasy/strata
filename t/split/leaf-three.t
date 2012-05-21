#!/usr/bin/env _coffee
require("./proof") 2, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/leaf-three.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  cursor = strata.mutator "b", _
  cursor.insert "b", "b", ~ cursor.index, _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  for i in [cursor.offset...cursor.length]
    records.push cursor.get i, _
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i" ], "records"

  strata.balance _

  #records = []
  #cursor = strata.iterator "a", _
  #for i in [cursor.offset...cursor.length]
  #  records.push cursor.get i, _
  #cursor.unlock()

  #@deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i" ], "records"

  expected = load "#{__dirname}/fixtures/leaf-three.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "split"
