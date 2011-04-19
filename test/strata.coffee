{Strata, MemoryIO}    = require "strata"

class exports.PacketTest extends TwerpTest
  "test: add an item": (done) ->
    strata = new Strata(new MemoryIO(), [])
    strata.insert('a')
    strata.find('a')
    @ok strata
    done 1
