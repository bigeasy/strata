#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 5, ({ Strata, directory }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.create(_)
  strata.close(_)
  @ok 1, "created"
  lines = fs.readFile("#{directory}/segment00000000", "utf8", _).split(/\n/)
  lines.pop()
  @equal lines.length, 1, "root lines"
  @deepEqual JSON.parse(lines[0]), [ true, -1, [ 1 ] ], "root"
  lines = fs.readFile("#{directory}/segment00000001", "utf8", _).split(/\n/)
  lines.pop()
  @equal lines.length, 1, "leaf lines"
  @deepEqual JSON.parse(lines[0]), [ 0, -1, [] ], "leaf"
