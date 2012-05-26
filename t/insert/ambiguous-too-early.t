#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 3, ({ Strata, directory, fixture: { serialize, load, objectify } }, _) ->
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

  unambiguous = []
  cursor = strata.mutator "a", _
  loop
    unambiguous.unshift(cursor.insert "z", "z", ~cursor.indexOf("z", _), _)
    break if unambiguous[0]
    cursor.next _
  cursor.unlock()

  @deepEqual unambiguous, [ true, false, false, false ], "unambiguous"

  records = []
  cursor = strata.iterator _
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get i, _
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "d", "f", "g", "h", "i", "l", "m", "n", "z" ], "records after insert"
