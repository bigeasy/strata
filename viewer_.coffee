{Strata, InMemory}    = require "./lib/strata"
{OptionParser}  = require "coffee-script/lib/coffee-script/optparse"

parser = new OptionParser [
  [ "-f", "--file [NAME]", "strata file" ]
  [ "-c", "--create", "create database" ]
  [ "-a", "--alpha [VALUE]", "add letters of the alphabet" ]
  [ "-g", "--get [VALUE]", "get a value" ]
  [ "-i", "--insert [VALUE]", "a string value to insert" ]
  [ "-d", "--delete [VALUE]", "a string value to delete" ]
  [ "-h", "--help", "display help" ]
]

usage = (message) ->
  process.stderr.write "error: #{message}\n"
  process.stderr.write parser.help()
  process.stderr.write "\n"
  process.exit 1

try
  options         = parser.parse process.argv.slice(2)
catch e
  usage "Invalid arguments."

view = (_) ->
  strata = new Strata directory: "./database", leafSize: 3, branchSize: 3
  if options.create
    strata.create(_)
  else
    strata.open(_)
  if value = options.insert
    strata.insert value, _
  else if value = options.get
    console.log strata.get value, _

view (error) -> throw error if error
