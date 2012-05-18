#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 4, ({ Strata, directory, fixture: { serialize } }, _) ->
  serialize "#{__dirname}/fixtures/nine.json", directory, _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  cursor = strata.iterator("a", _)
  @equal cursor.index, 0, "index"
  @equal cursor.offset, 0, "offset"
  @equal cursor.length, 3, "length"

  records = []
  loop
    for i in [cursor.offset...cursor.length]
      records.push cursor.get(i, _)
    break unless cursor.next(_)
  cursor.unlock()

  @deepEqual records, [ "a", "b", "c", "d", "e", "f", "g", "h", "i" ], "records"
