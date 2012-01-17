#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 3, ({ Strata, directory }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.create _
  strata.insert "a", _
  strata.close _
  lines = fs.readFile("#{directory}/segment00000001", "utf8", _).split(/\n/)
  lines.pop()
  @equal lines.length, 2, "leaf lines"
  @deepEqual JSON.parse(lines[0]), [ 0, -1, [] ], "positions array"
  @deepEqual JSON.parse(lines[1]), [1,"a"], "insert object"
