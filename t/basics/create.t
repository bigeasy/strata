#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 4, ({ Strata, directory, fixture: { load, objectify } }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.create _
  @equal strata._io.size, 4, "json size"
  strata.close _
  @ok 1, "created"

  expected = load "#{__dirname}/fixtures/create.after.json", _
  actual = objectify directory, _

  @say expected
  @say actual

  @deepEqual actual, expected, "written"

  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open _
  cursor = strata.iterator "a", _
  @equal cursor.length - cursor.offset, 0, "empty"
  cursor.unlock()
  strata.close _
