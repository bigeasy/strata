class Mutation
  constructor: (root, initial, subsequent, swap, penultimate) ->
    @levels = []
    @decisions = { root, initial, subsequent, swap, penultimate }

class Level
  constructor: (@exclusive) ->

generalized = (mutation, decisions) ->
  mutation.levels.push new Level(false)
  root = @io.root @rootAddress

class module.exports.Strata
  constructor: (options) ->
    @leafSize = options.leafSize or 12
    @branchSize = options.branchSize or 12
    throw new Error("I/O Strategy is required.") unless @io = options.io
    throw new Error("Root address is required.") unless @rootAddress = options.rootAddress

  add: (object, callback) ->
    fields = @io.fields object
    mutation = { fields, object }
    @_mutation new Mutation(shouldSplitRoot)

  @_mutation: (mutation, decisions) ->
    @_queue.push mutation
    @_queue.push []
    @_dequeue()
   
  @_dequeue: ->
    if queue.length % 2 is 1
      while queue[0].length isnt 0
        reader = queue[0].shift()
        @_reading++
        reader.read =>
          @_reading--
          @_dequeue()
      if queue.length > 1 and reading is 0
        queue.shift()
        @_enqueue()
