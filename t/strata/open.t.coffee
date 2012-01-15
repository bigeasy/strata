#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
require("./harness") 1, ({ Strata, directory }, _) ->
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.create(_)
  strata.close(_)
  strata = new Strata directory: directory, leafSize: 3, branchSize: 3
  strata.open(_)
  @equal strata._io.nextAddress, 2, "next address"
