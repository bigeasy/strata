#!/usr/bin/env _coffee
fs = require "fs"
require("./harness") 5, ({ Strata, directory }, _) ->
  fs.writeFile "#{directory}/segment00000000", "#{JSON.stringify([-1,[-1]])}\n", "utf8", _
  fs.writeFile "#{directory}/segment00000001", """
    #{JSON.stringify([0,-1,[]])}
    #{JSON.stringify([1,"a"])}

  """, "utf8", _

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)

  @equal strata._io.size, 0, "json size"

  cursor = strata.iterator "a", _

  @ok not cursor.exclusive, "shared"
  @equal cursor.index, 0, "index"

  @ok cursor.found, "found"
  @equal cursor.get(cursor.index, _), "a", "get"

  cursor.unlock()
