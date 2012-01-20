#!/usr/bin/env coffee-streamline
return if not require("streamline/module")(module)
fs = require "fs"
objectify = module.exports.objectify = (directory, _) ->
  segments = {}
  for file in fs.readdir directory, _
    continue if /^\./.test file
    lines = fs.readFile("#{directory}/#{file}", "utf8", _).split(/\n/)
    lines.pop()
    segments[file] = (JSON.parse(json) for json in lines)
  segments
stringify = module.exports.stringify = (directory, _) ->
  segments = objectify directory, _
  console.log(JSON.stringify(segments, null, 2))

load = module.exports.load = (segments, _) ->
  JSON.parse fs.readFile segments, "utf8", _

module.exports.serialize = (segments, directory, _) ->
  if typeof segments is "string"
    segments = load segments, _
  for file, lines of segments
    lines = (JSON.stringify(line) for line in lines)
    lines.push ""
    fs.writeFile "#{directory}/#{file}", lines.join("\n"), "utf8", _
