#!/usr/bin/env _coffee
require("./proof") 3, ({ Strata, directory, fixture: { load, objectify, serialize } }, _) ->
  serialize "#{__dirname}/fixtures/root-drain.before.json", directory, _

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

  @deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" ], "records"

  strata.balance _

  #records = []
  #cursor = strata.iterator "a", _
  #for i in [cursor.offset...cursor.length]
  #  records.push cursor.get i, _
  #cursor.unlock()

  #@deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i" ], "records"

  expected = load "#{__dirname}/fixtures/root-drain.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "split"

  strata.close _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _

  records = []
  cursor = strata.iterator _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j" ], "records"
