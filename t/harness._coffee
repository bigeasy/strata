fs        = require "fs"
fixture   = require "./fixture"
{Strata}  = require "../lib/strata"

module.exports = (dirname) ->
  require("ace.is.aces.in.my.book") (_) ->
    directory = "#{dirname}/tmp"
    try
      fs.mkdir directory, 0755, _
    catch e
      throw e if e.code isnt 'EEXIST'
    for file in fs.readdir directory, _
      continue if /^\./.test file
      fs.unlink "#{directory}/#{file}", _
    { Strata, directory, fixture }
