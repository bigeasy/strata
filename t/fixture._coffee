fs = require "fs"
crypto = require "crypto"

objectify = module.exports.objectify = (directory, _) ->
  segments = {}
  for file in fs.readdir directory, _
    continue if /^\./.test file
    lines = fs.readFile("#{directory}/#{file}", "utf8", _).split(/\n/)
    lines.pop()
    segments[file] = []
    for json in lines
      json = json.replace /[\da-f]+$/, ""
      segments[file].push JSON.parse(json)
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
    records = []
    for line in lines
      record = [ JSON.stringify(line) ]
      record.push checksum = crypto.createHash("sha1").update(record[0]).digest("hex")
      records.push record.join " "
    fs.writeFile "#{directory}/#{file}", records.join("\n") + "\n", "utf8", _
