#!/usr/bin/env _coffee
fs = require "fs"
require("./proof") 6, ({ Strata, directory }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.create(_)
  @equal strata._io.size, 4, "json size"
  strata.close(_)
  @ok 1, "created"
  lines = fs.readFile("#{directory}/segment00000000", "utf8", _).split(/\n/)
  lines.pop()
  @equal lines.length, 1, "root lines"
  @deepEqual JSON.parse(lines[0]), [ 0, [ -1 ] ], "root"
  lines = fs.readFile("#{directory}/segment00000001", "utf8", _).split(/\n/)
  lines.pop()
  @equal lines.length, 1, "leaf lines"
  @deepEqual JSON.parse(lines[0]), [ 0, 0, 0, [] ], "leaf"
