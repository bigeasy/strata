{TwerpTest} = require "twerp"
{Strata, InMemory}    = require "../lib/strata"

class exports.PacketTest extends TwerpTest
  "test: construct strata": (done) ->
    strata = new Strata
    @ok strata
    done 1

  "test: insert object": (done) ->
    strata = new Strata
    @ok strata
    strata.insert 1, (error, altered) =>
      @ok altered
      done 2
