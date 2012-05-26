#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 4, ({ Strata, directory, fixture: { serialize, load, objectify } }, _) ->
  serialize "#{__dirname}/fixtures/ambiguous.before.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d", "f", "g", "h", "i", "l", "m", "n" ], "records"

  cursor = strata.mutator "g", _
  cursor.delete cursor.index, _
  cursor.unlock()

  records = []
  cursor = strata.iterator "a", _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d", "f", "h", "i", "l", "m", "n" ], "records after delete"

  cursor = strata.mutator "j", _
  unambiguous = cursor.insert "j", "j", ~cursor.index, _
  cursor.unlock()

  @ok unambiguous, "unambiguous"

  records = []
  cursor = strata.iterator _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d", "f", "h", "i", "j", "l", "m", "n" ], "records after insert"
