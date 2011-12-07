{TwerpTest} = require "twerp"
{Strata, InMemory}    = require "./lib/strata"
{OptionParser}  = require "coffee-script/lib/optparse"

parser = new OptionParser [
  [ "-f", "--file [NAME]", "strata file" ]
  [ "-c", "--create", "create database" ]
  [ "-a", "--add [VALUE]", "a string value to add" ]
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

view (error) -> throw error if error
