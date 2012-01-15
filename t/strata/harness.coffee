#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
{Strata} = require "../../lib/strata"
module.exports = require("ace.is.aces.in.my.book") (_) ->
  directory = "#{__dirname}/../../tmp/strata"
  try
    fs.mkdir "#{__dirname}/../../tmp", 0755, _
  catch e
    throw e if e.code isnt 'EEXIST'
  try
    fs.mkdir directory, 0755, _
  catch e
    throw e if e.code isnt 'EEXIST'
  for file in fs.readdir directory, _
    continue if /^\.\.?$/.test file
    fs.unlink "#{directory}/#{file}", _
  { Strata, directory }
