fs        = require "fs"
fixture   = require "./fixture"
{Strata}  = require "../lib/strata"

module.exports = (dirname) ->
  require("proof") (_) ->
    directory = "#{dirname}/tmp"
    deltree = (file, _) ->
      try
        stat = fs.stat file, _
        if stat.isDirectory()
          for entry in fs.readdir file, _
            deltree "#{file}/#{entry}", _
          fs.rmdir file, _
        else
          fs.unlink file, _
      catch e
        throw e if e.code isnt "ENOENT"
    @cleanup _, (_) -> deltree(directory, _)
    fs.mkdir directory, 0755, _
    { Strata, directory, fixture }
