class Mutation
  constructor: (@strata, root, initial, subsequent, swap, penultimate) ->
    @levels = []
    @decisions = { root, initial, subsequent, swap, penultimate }

class Level
  constructor: (exclusive) ->
    @lock = if exclusive then @lockExclusive else @lockShared
  lockExclusive: (mutation, tier, callback) ->
    strata = mutation.strata
    locks = strata.locks
    if locks.exclusive[tier.id] or locks.shared[tier.id]
      @exclusiveQueue.push [ mutation, tier, callback ]
    else
      strata.locks.exclusive[tier.id]++
      callback()
  lockShared: (mutation, tier, callback) ->
    strata = mutation.strata
    if locks.exclusive[tier.id]
      @sharedQueue.push [ mutation, tier, callback ]
    else
      strata.locks.shared[tier.id]++
      callback()

class module.exports.MemoryIO
  root: (map) -> map
  fields: (object) -> object
  compare: (left, right) ->
    if left < right then -1 else if left > right then 1 else 0

class module.exports.Strata
  constructor: (options) ->
    @leafSize = options.leafSize or 12
    @branchSize = options.branchSize or 12
    throw new Error("I/O Strategy is required.") unless @io = options.io
    throw new Error("Root address is required.") unless @rootAddress = options.rootAddress
    @locks = { shared: {}, exclusive: {} }

  add: (object, callback) ->
    fields = @io.fields object
    @_mutation new Mutation(this, @_shouldSplitRoot)

  generalized: (mutation) ->
    mutation.levels.push new Level(false)
    mutation.parent = @io.root @rootAddress
    mutation.parentLevel = new Level(false)
    mutation.levels.push mutation.parentLevel
    parentLevel.lock mutation, @_initialTest
  
  _initialTest: (mutation) ->
    mutation.childLevel = new Level(false)
    mutation.levels.push mutation.childLevel
    if mutation.initial.call this, mutation
      mutation.parentLevel.operations.clear()
      mutation.parentLevel.upgrade mutation.childLevel, @_initialTestAgain

  _initialTestAgain: (mutation) ->
    parentLevel = mutation.levels[0]
    parent = @io.root @rootAddress
    if not mutation.initial.call this, mutation
      mutation.rewind 0
    @_tierDecision mutation

  _tierDecision: (mutation) ->
    if mutation.parent.penultimate
      @_testInnerTier mutation, mutation.penultimate, 1, @_operate
    else
      @_testInnerTier mutation, mutation.subsequent, 0, @_tierDescend

  _tierDescend: (mutation) ->
    branch = @_find(mutation.parent, mutation.sought)
    @io.load mutation.parent, branch, (child) =>
      mutation.parent = child
      mutation.parentLevel = mutation.childLevel
      mutation.childLevel = new Level(mutation.parentLevel.exclusive)
      mutation.levels.push mutation.childLevel
      mutation.shift @_tierDescend

  _find: (tier, sought) ->
    
  _testInnerTier: (mutation, decision, leaveExclusive, next) ->
    tiers = decision.test.call mutation
    keys = mutation.swap.test.call mutation
    if tiers or keys
      if not (mutation.parentLevel.exclusive and mutation.childLevel.exclusive)
        mutation.parentLevel.upgrade mutation.childLevel, =>
          mutation.parentLevel.operations.clear()
          mutation.childLevel.operations.clear()
          @_testInnerTier mutation, leaveExclusive, next
      else if not tiers
        mutation.rewind leaveExclusive
        next.call mutation.strata, mutation
      else
        next.call mutation.strata, mutation
    else
      mutation.rewind leaveExclusive
      next.call mutation.strata, mutation

  _shouldSplitRoot: (mutation)
    if @branchSize is mutation.parent.length
      mutation.parentLevel.operations.add(@_splitRoot)
      true
    else
      false

  _mutation: (mutation) ->
    @_queue.push mutation
    @_queue.push []
    @_dequeue()
   
  _dequeue: ->
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
