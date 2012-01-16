# A Streamline.js friendly evented I/O b-tree for node.js.
#
# ## Purpose
# 
# Implements a file backed [b-tree](http://en.wikipedia.org/wiki/B-tree).
#
# TK Points to cover as far as advocacy goes.
#
#  * Concurrency.
#  * Flexiblity of ordering.
#  * Atomicity.
#  * A database primitive, for use as if it were a programming primitive like an
#  array or an associative array.
#  * Standard Node.js API.
#
# But this isn't important at the moment.
#
# ### Terminology
#
# We refer to the nodes in our b-tree as *pages*. The term node conjures an
# image of a discrete component in a linked data structure that contains one,
# maybe two or three, values. Nodes in a b-tree contain hundreds or thousands of
# values. They are indexed. They are read from disk. They are allowed to fall
# out of memory when they have not been recently referenced. These are behaviors
# that conjure an image of a page of values.
#
# Otherwise, we use common teriminology for *order*, *depth*, *parent*, *child*,
# *split* and *merge*. There is no hard and fast definition for all the terms. A
# leaf is a fuzzy concept in b-tree literature, for example. We call a page that
# contains records a *leaf page*. We call a non-leaf page a *branch page*.
#
# The term *b-tree* itself may not be correct. There are different names for
# b-tree that reflect the variations of implementation, but those distinctions
# have blurred over the years. Our implemenation may be considered a b+tree,
# since pages are linked, and records are stored only in the leaves.
#
# Terms specific to our implementation will be introduced as they are
# encountered in the document. 
#
# ### What is a b-tree?
#
# This documentation assumes that you understand the theory behind the b-tree,
# and know the variations of implementation. If you are interested in learning
# about b-trees, this document could be a learning aid for you, but you should
# start with the Wikipedia articles on
# [B-trees](http://en.wikipedia.org/wiki/B-tree) and
# [B+trees](http://en.wikipedia.org/wiki/B%2B_tree). 
#
# ### What flavor of b-tree is this?
#
# Strata is a b-tree with leaf pages that contain records ordered by the
# collation order of the tree, indexed for retrieval in constant time, so that
# they can be found using binary search. Branch pages contain links to other
# pages, and do not store records themselves. Leaf pages are linked in ascending
# order to ease the implementation of cursors.
#
# There is a maximum size for a page, that limits the number of links, in the
# case of branch pages, or records in the case of leaf pages. When the maximum
# size is reached which point a node is split. When two sibling pages next to
# each other contain keys or records that combined are the less than than
# maximum size they are merged.
#
# The tree always has a root branch page. The order of the tree increases when
# the root branch is split. It decreases when the root branch is merged. The
# split of the root branch is a different operation from the split of a non-root
# branch, because the root branch does not have siblings.
#
# ### Page Structure
#
# The b-tree has two types of pages. Leaf pages and branch pages.
#
# #### Leaf Pages
#
# Leaf pages contain records.
#
# In the abstract, a leaf page is an array data structure with zero based
# integer index. The elements of the structure contain records. Given an
# integer, the leaf page will return the record stored at element in the array.
# This lookup is performed in constant time.
# 
# The records in the record array are ordered according to the collation of the
# b-tree. Because of this, and because a lookup takes constant time, we can search
# for a record in a leaf page using binary search in logorithmic time.
#
# The first record of every leaf page must be unique in relation to the first
# record of every other leaf page. The b-tree will accept records that are
# dupcliates according to the collation. When we split a page, we cannot cannot
# choose a partition that has the same value as the first record, since that
# would violate the unique first record constraint.
#
# If a page reaches the maximum leaf page size, filled with records that have
# the same value according to the collation, the leaf page is cannot split,
# since no suitable partition exists. The page will be allowed to grow beyond
# the maximum page size.
#
# #### Branch Pages
#
# Branch pages contain links to other pages. To find the a record in the b-tree,
# we first use branch pages to find the leaf page that contains our record.
#
# All pages are identified by a unique page address. The page address is an
# integer assigned to the page when the page is created. Branch pages link to
# other pages by storing the address of the referenced page in an array of page
# addresses. This array of page addresses is essentially an *array of children*.
#
# A branch page orders its children according to the b-tree collation using a
# record obtained from a leaf page as a *key*. A branch page always has one more
# children than the number of keys. Keys are unique. A key used in a branch page
# will not equal another key used in any branch page according to the collation.
#
# There are two special types of branch pages, the root page, and penultimate
# pages.
#
# ##### Root Page
#
# The root page is the first page we consult to find the desired leaf page. Our
# b-tree always contains a root page. The b-tree is never so empty that the root
# page disappears. The root page always has the same address.
#
# TK move. Until the root branch page is split, it is both the root branch page
# and a penultimate branch page.
#
# ##### Penultimate Pages
#
# A penultimate branch page is a branch page whose children are leaf pages. If a
# branch page is not penultimate, then its children are branch pages.
#
# In a penultimate branch page, the array of children is ordered by the b-tree
# collation using a first record in the referenced leaf page for ordering. That
# is, the first record of the leaf page is used as the key associated with a
# page address in a penultimate branch page.
#
# The non-leaf nodes of a b-tree have the property that the number of node
# children is one greater than the number of keys. We obtain this property by
# treating the first child as the left child of the entire page, and excluding
# its key from the search. We search the subsequent keys to find the first key
# that is grater than or equal to the record sought. Essentially, when we
# encouter a key that is greater than our sought record, we know that the record
# is contained in the leaf page child associated with the key before it.
# Although it sounds linear, we are able to perform this search using binary
# search in logorithmic time.
#
# By ignoring the key of the first leaf page, the penultimate branch page has a
# number of children that is one greater than the number of keys.
#
# Notice that, when we are inserting a record into a leaf page other than the
# left leaf page, we add it to a leaf page whose key is equal to or greater than
# the penultimate branch key, so that the first record does not change, and
# therefore that penultimate branch key does not change. The exception is the
# left leaf page, which accepts all the records less than the first key, and
# therefore may accept a record less than its current least record.
#
# An insertion can only insert a into the left most leaf page of a penumltimate
# branch page a record less than the least record of the leaf page.
#
# ##### Interior Branch Pages
#
# A branch page whose children are other branch pages is called an interior
# branch page.
#
# Like the penultimate branch page, we treat the first child of an interior
# branch page as the left child of the entire page. Like the penultimate branch
# page the subsequent children have an associated key that is the first record
# of a leaf page.
#
# The key is obtained by decending the sub-tree referenced by the child. We
# first visit the branch page referneced by the child. We then visit left
# children recursively, visiting the left child of the child, and the left child
# of any subsquently visited children, until we reach a leaf page.  The first
# record of that leaf page is the key to associate with the child address in the
# address array of the interior branch page.
#
# It is the nature of the b-tree that keys move up to the higher levels of the
# tree as pages split, while preserving the collation order of the keys. When a
# branch page splits, a key from the middle of the page is chosen as a
# partition. The partition is used as the key for the right half of the split
# page in the parent page.
#
# Our implementation does not store the keys, as you may have noticed, but
# decends down to the leaf page to fetch the record to use as a key. 
#
# We start from a penultimate page as a root page. When a leaf page fills, we
# split it, creating a new right leaf page. The penultimate page uses the first
# record of the new right page as the key to associate with that page.
#
# When the root penultimate page is full we split it, so that the root page is
# an interior page with two children, which are two penultimate pages. The tree
# now contains a root interior branch page, with a left penultimate branch page
# and a right penultimate branch page.
#
# The root interior branch page has one key. Prior to split, that key was
# associated with the address of a child leaf page. After split, the key is
# associated with the right penultimate branch page. The leaf page is now the
# left child of the right penultimate branch page.
#
# When we visit the root interior page, to obtain the key to associate with the
# right penultimate page, we visit the right penultimate page, then we visit its
# left child, the leaf page whose first record is the key.
#
# #### Keys and First Records
#
# We said that it is only possible for an insertion to insert a into the left
# most child leaf page of a penumltimate branch page a record less than the least
# record. We can say about a tree rooted by an interor branch page, that an
# insertion is only able to insert into the left most leaf page in the *entire
# tree* a record less than the least record.
#
# Using our example tree with one root interior page, with two penultimate
# branch page children, we cannot insert a record into the right penultimate
# branch page that will displace the first record of its left most child branch,
# because that first record is the key for the right penultimate branch page.
# When we insert a record that is less than the key, the search for a leaf page
# to store the record goes to the left of the key. It cannot descend into the
# right penultimate branch page, so it is impossible for it be inserted into
# left child of the right penultimate branch page, so the first record left
# child of the penultimate branch page will never be displaced by an insertion.
#
# Only if we insert a record that is less than least key of the left penultimate
# page do we face the possibility of displacing the first record of a leaf page,
# and that leaf page is the left most leaf page in the entire tree.
#
# This maintains a property of the b-tree that for every leaf page except the
# left most leaf page, there exists a unique branch page key derived from the
# first record of the page.
#
# As above, you can find the first record used to derive a key by visting the
# child and going left. You can find the leaf page to the left of the leaf page
# used to derive a page branch key, by visiting the child to the left of the key
# and going right.
#
# NOTE Literate programming has finally materialized with Docco and
# CoffeeScript.
#
# When the root page splits, it becomes an interior branch page. Until it splits
# it is both the root page and a penultimate page.

# Requried node.js libraries.
fs = require "fs"

# Copy values from one hash into another.
extend = (to, from) ->
  to[key] = value for key, value of from
  to

# Useful for command debugging. If you don't see them called in the code, it
# means the code is absolutely bug free.
die = (splat...) ->
  console.log.apply null, splat if splat.length
  process.exit 1
say = (splat...) -> console.log.apply null, splat

# ### Collation
#
# A b-tree has a client defined collation. The collation is determined by the
# combination of an extractor and a comparator. The extractor is used to
# extract or calculate from the stored record the fields values pertient to the
# collation. The comparator is used to order records by comparing the extracted
# fields.
#
# The extractor allows us to cache the fields pertinent to the collation. It may
# be the case that records have large fields that are not pertinent to the
# collation. If we are only using the record as a point of reference while
# searchng the tree, we allow the record to fall out of the cache, and hold onto
# only the pertient fields. 
#
# This strategy wouldn't work if extraction and comparison were the same
# function. By making them separate functions, we can cache the intermediate
# extraction step.
#
# Additionally, the comparitor is pretty easily generalized, while the exractor
# is invariably specialized.

# Default comparator is good only for strings, use a - b for numbers.
comparator = (a, b) ->
  if a < b then -1 else if a > b then 1 else 0

# Default extractor returns the value as hole, i.e. tree of integers.
extractor = (a) -> a

# Glossary:
# 
#  * Descent - What we call an attempt to move from a parent node to a child
#              node in  the tree, which may be paused because it encounters a
#              lock on a tier.
#  * Record - A JSON object stored in the b-tree.
#  * Object - JavaScript objects are referred to as objects in this
#             documentation and are not to be confused with actual records.

#              
class Mutation
  # The constructor always felt like a dangerous place to be doing anything
  # meaningful in C++ or Java.
  constructor: (@strata, @object, @key, @operation) ->
    @io         = @strata._io
    @options    = @strata._options

    @parent     = { operations: [], locks: [] }
    @child      = { operations: [], locks: [] }
    @exclusive  = []
    @shared     = []
    @pivots     = []

    { @extractor, @comparator } = @options

    @key        = @extractor @object if @object? and not key?
  
  # Descend the tree, optionally starting an exclusive descent. Once the descent
  # is exclusive, all subsequent descents are exclusive.

  # `mutation.descend(exclusive)` 
  descend: (exclusive) ->
    # Add a new level to the plan.
    @plan.unshift { operations: [], addresses: [] }

    # Two or more exclusive levels mean that the parent is exclusive.
    if @exclusive.length < 1
      @strata._release @parent

    # Any items in the exclusive level array means we are now locking exclusive.
    exclusive or= @exclusive.length > 0

    # Make child a parent.
    @parent = @child
    @child = { operations: [], locks: [], exclusive }

    # Add the new child to the list of exclusive levels.
    if exclusive
      @exclusive.push(@child)

  # Search for a value in a tier, returning the  index of the value or else
  # where it should be inserted.
  #
  # There is some magic here, long forgotten at the time of documentation. The
  # tree beneath each branch in an inner tier contains records whose value is
  # equal to or greater than the pivot. Thus, the pivot of the first record on
  # an inner tier is null, indicating that that is the branch for all values
  # less than the value of the first branch with a real pivot.
  #
  # TODO: Reading through the code, this does not take this into account, and
  # will problably send items that belong in the least value tree into the three
  # that follows it. I'll clean this up with unit testing.

  # `mutation.find(tier, key, callback)`
  find: (tier, key, _) ->
    size = tier.addresses.length
    if tier.leaf
      [ low, high, leaf ] = [ 0, size - 1, true ]
    else if size is 1
      return 0
    else
      [ low, high ] = [ 1, size - 1 ]
    { comparator, io } = @
    loop
      mid = (low + high) >>> 1
      compare = comparator key, io.key(tier, mid, _)
      if compare is 0
        break
      if compare > 0
        low = mid + 1
      else
        high = mid - 1
    if compare is 0
      if leaf
        while mid != 0 && comparator(key, io.key(tier, mid - 1, _)) == 0
          mid--
      mid
    else if branches
      mid - 1
    else
      mid

  hasKey: (tier, _) ->
    for key in @operation.keys or []
      die { key }
      branch = @find(tier, key, _)
      if tier.cache[branch] is key
        return true
    false

  # TODO: We are always allowed to get a shared lock on any leaf page and read a
  # value, as long as we let go of it immediately. This allows us to read from
  # leaf pages to get branch values.
  # TODO: Rename this descend.

  # `mutation.mutate(callback)`
  mutate: (_) ->
    parent = null

    @shared.push child = @io.lock false, 0, false, _
    if @operation.keys and (child.addresses.length is 1 or @hasKey(child, _))
      @exclusive.push @io.upgrade @shared.pop(), _

    # TODO: Flag for go left.
    while not child.penultimate or child.address is @soughtAddress
      process.exit 1

    # TODO: Flag for shared or exclusive. If exclusive, leave parent locked, if
    # shared, then unlock parent.

    mutation = @[@operation.method].call this, parent, child, _

    @io.release tier for tier in @shared
    @io.release tier for tier in @exclusive

    mutation.mutate _ if mutation

  insertSorted: (parent, child, _) ->
    branch = @find child, @key, _
    @exclusive.push leaves = @io.lock true, child.addresses[branch], true, _

    addresses = leaves.addresses

    # Insert the object value sorted.
    for i in [0...addresses.length]
      key = @io.key(leaves, i, _)
      if @comparator(@key, key) <= 0
        break

    address = leaves.io.writeInsert @object, i, _
    addresses.splice i, 0, address

    if addresses.length > @options.leafSize and not @homogenous(leaves, _)
      keys = []
      process.nextTick _
      # Opportunity to deadlock.
      keys.push key = @io.key leaves, 0, _
      if leaves.right
        keys.push @io.key leaves.right, 0, _
      operation =
        method: "splitLeaf"
        keys: keys
      new Mutation(@strata, null, key, operation)
  
  # If the leaf tier is now at the maximum size, we test to see if the leaf tier
  # is filled with an identical key value, and if not, we split the leaf tier.
  homogenous: (tier, _) ->
    { io, comparator } = @
    first = io.key(tier, 0, _)
    last = io.key(tier, tier.addresses.length - 1, _)
    comparator(first, last) is 0

  get: (parent, child, _) ->
    branch = @find child, @key, _
    @shared.push leaves = @io.lock false, child.addresses[branch], true, _
    address = @find leaves, @key, _
    leaves.cache[leaves.addresses[address]]

  splitLeaf: (parent, child, _) ->
    branch = @find child, @key, _
    @exclusive.push leaves = @io.lock true, child.addresses[branch], true, _

    # See that nothing has changed since we last descended.
    { comparator: c, io, options: { leafSize: length } } = @
    return if c(io.key(leaves, 0, _), @key) isnt 0
    return if @homogenous(leaves, _)
    return if leaves.addresses.length < @options.leafSize

    # We might have let things go for so long, that we're going to have to split
    # the page into more than two pages.
    partitions = Math.floor leaves.addresses.length / @options.leafSize
    while (partitions * @options.leafSize) <= leaves.addresses.length
      # Find a partition.
      mid = Math.floor leaves.addresses.length / (partitions + 1)
      key = io.key leaves, mid, _
      [ before, after ] = [ mid - 1, mid + 1 ]
      while not partition
        if before > 0 and c(io.key(leaves, before, _), key) isnt 0
          partition = before + 1
        else if after <= length and c(io.key(leaves, after, _)) isnt 0
          partition = after
        else
          --before
          ++after

      key = io.key leaves, partition, _
      pivot = @find(child, key, _) + 1

      addresses = leaves.addresses.splice(partition)
      right = io.allocateLeaves(addresses, leaves.right)

      leaves.right = right.address

      child.addresses.splice(pivot, 0, right.address)

      # Append an operation indciator. This would be a record that describes all
      # the participants in the rewrite, in the order in which they are supposed
      # to be rewritten. When found, we look at the last item in the list, then
      # load that, and ensure that the last record is a complete operation
      # record. If it is, we know that all of the temporary pages were written.
      @io.rewriteLeaves(leaves, _)
      @io.rewriteLeaves(right, _)

      @io.rewriteBranches(child, _)

      @io.relink(right, _)
      @io.relink(leaves, _)
      @io.relink(child, _)

      --paritions

  mergeLeaf: (parent, child, _) ->
    [ key, rightKey ] = @operation.keys
    compare = @comparator(full, leftKey = @io.key(child, 0, _))
    if compare is 0
      return if leftKey isnt key

class Level
  # Construct a new level. The `exclusive` pararameter determines how this level
  # locks tiers when asked to lock tiers.
  constructor: (@exclusive) ->
    @operations = []
    @locks = []

  # Convert lock to read lock and tell anyone waiting that they can go ahead.
  #
  # We give the next tick a copy of just the continuations to fire, excluding
  # our own continuation which we're about to add. (TODO rename continuations?) 
  #
  # If you're wondering, no, you're not going to fire the continuations twice.
  # They are only ever fired by the decent that holds the exclusive lock. This
  # level will be removed from the level list, so we won't grab at it when the
  # operations are over.
  downgrade: (strata) ->
    if @exclusive
      while @locks.length
        lock = @locks.shift()
        locks = strata.locks[lock.address]
        throw "not locked by me" if locks[0][0] isnt lock
        locks.shift()
        @_resume strata, locks[0].slice(0)
        locks[0].push lock
      @exclusive = false
  
  # Unused.
  advance: (strata) ->
    locks = strata.locks[@tier.id]
    if locks[0].length is 0 and locks.length isnt 1
      locks.shift()
      true
    false

# ### B-Tree Structure
#
# We call page of the b-tree, ah, maybe we stop using the word tier, start using
# the word page, because we're not going to have to reduce the ambiguity with
# some sort of subdivided binary file structure.
#
# NOTE Come back and look at this. It is succinct.
#
# There are two types of pages. Branch pages and leaf pages.
#
# Leaf pages store the records. They maintain the collation
#
# Leaf pages are all uniformly the same.
#
# ### Leaf Tier Files
#
# A leaf tier maintains an array of file positions called an address array. A
# file position in the address array references a record in the leaf tier file
# by its file position. The addresses in the address array are sorted by the
# the sort order of the referenced records according to the sort order of the
# b-tree.
#
# A leaf tier file contains JSON objects, one object on each line.
#
# There are three types of objects in a leaf tier file, insert objects, delete
# objects, and address array objects.
#
# An insert object contains a *record* and the index in the address array where
# the record's address would be inserted to preserve the sort order of the
# address array.
#
# Beginning with an empty array and reading from the start of the file, the leaf
# tier address array is reconstituted by reading the object at the current
# position in the file, then inserting the position of the current object into
# the address array at the index in the insert object.
#
# We record a deletion by appending a delete object. A delete object contains
# an index in the address array that should be deleted. When reading the leaf
# tier from the start of the file, we insert an address into the address array
# when we encounter an insert object, and we delete an address when we encounter
# a delete object.
#
# On occasion, we can store an address array object, An address array object
# contains the address array itself.  We store a copy of a constructed address
# array object in the leaf tier file so that we can read a large leaf tier file
# quickly.
#
# When we read a leaf tier file, if we read from the back of the file toward the
# front, we can read backward until we find an address array object. Then we can
# read forward to the end of the file, applying the inserts and deletes that
# occured after we wrote the address array object. 
# 
# When a leaf tier file is large, stashing the constructed address array at the
# end means that the leaf tier can be loaded quickly, because we will only have
# to read backwards a few entries to find a mostly completed address array. We
# can then read forward from the array to amend the address array with the
# inserts and deletes that occured after it was written.
#
# Not all of the records will be loaded when we go backwards, but we have their
# file position from the address array, so we can jump to them and load them as
# we need them. That is, if we need them, because owing to binary search, we
# might only need a few records out of a great many records to find the record
# we're looking for.
#
# Over time, a leaf tier file can grow fat with deleted records &mdash; an
# insert object and a subsequent delete object. We vacuum a leaf tier file by
# writing it to a new leaf tier file.

# `LeafIO` &mdash; Keeps track of a file descriptor opened for append, and the
# append file position. The `LeafIO` class does *not* contain all methods
# relating to leaf tier file I/O. You can find more leaf tier file methods in
# the `IO` class below.
#
# TODO Rename `RecordIO`.
class LeafIO
  # Construct a leaf file tier writer for the given file name.
  constructor: (@filename) ->
    @length = 1024

  # #### Leaf Tier File Object Format
  #
  # Each leaf tier object is a JSON array. The first element of the array is an
  # integer.
  #
  # If the integer is greater than zero, it indicates an insert object.  The
  # integer is the one based index into the zero based address array, indicating
  # the index where the position of the current insert object should be
  # inserted. The second element of the leaf tier object array is the record
  # object.

  # Write an insert object.
  writeInsert: (object, index, _) ->
    @_writeJSON [ index + 1, object ], _

  # If the integer is less than zero, it indicates a delete object. The absolute
  # value of the integer is the one based index into the zero based addrss
  # array, indicating the index of address array element that should be deleted.
  # There are no other elements in the leaf tier object array.

  # Write a delete object.
  writeDelete: (index, _) ->
    @_writeJSON [ -(index + 1) ], _
  
  # If the integer is zero, it indicates an address array object. The integer
  # value of zero is used only as an indicator. The second element is the
  # address of the leaf tier to the right of current leaf tier. The third
  # element of the leaf tier object array is the address array.

  # Write an address array object.
  writeAddresses: (right, addresses, _) ->
    @_writeJSON [ 0, right, addresses ], _

  # Append an object to the leaf tier file as a single line of JSON.
  _writeJSON: (object, _) ->
    @fd           or= fs.open @filename, "a+", 0644, _
    @position     or= fs.stat(@filename, _).size

    json            = JSON.stringify object
    position        = @position
    length          = Buffer.byteLength(json, "utf8")
    buffer          = new Buffer(length + 1)
    offset          = 0

    # Write JSON and newline.
    buffer.write json
    buffer[length] = 0x0A

    # Write may be interrupted by a signal, so we keep track of how many bytes
    # are actually written and write the difference if we come up short.
    while offset != buffer.length
      count = buffer.length - offset
      written = fs.write @fd, buffer, offset, count, @position, _
      @position += written
      offset += written

    # What's the point if it doesn't make it to disk?
    fs.fsync @fd, _

    # Return the file position of the inserted record.
    position

  # Each line is terminated by a newline. In case your concerned that this
  # simple search will mistake a byte inside a multi-byte character for a
  # newline, have a look at
  # [UTF-8](http://en.wikipedia.org/wiki/UTF-8#Description). All bytes
  # participating in a multi-byte character have their leading bit set, all
  # single-byte characters have their leading bit unset.

  # Search for the newline that separates JSON records.
  _readJSON: (buffer, read) ->
    for offset in [0...read]
      if buffer[offset] is 0x0A
        return JSON.parse buffer.toString("utf8", 0, offset + 1)

  # Jump to a position in the file and read a specific object. Because objects
  # are cached in memory by a leaf tier, we're not going to get a request to
  # read an object that has been written to the current write stream. When we
  # read in a leaf tier, we'll cache any objects we encounter prior to read the
  # ordering array.
  #
  # Note how we allow we keep track of the minimum buffer size that will
  # accommodate the largest possible buffer.
  readObject: (address, _) ->
    @fd or= fs.open @filename, "a+", _
    @position or= fs.stat(@filename, _).size
    loop
      buffer = new Buffer(@length)
      read = fs.read @in, buffer, 0, buffer.length, address, _
      if json = @_readJSON(buffer, read)
        break
      if @length > @position - address
        throw new Error "cannot find end of record."
      @length += @length >>> 1
    json.pop()

  # Close the file descriptor if it is open and reset the file descriptor and
  # append position.
  close: (_) ->
    fs.close(@fd, _) if @fd
    @position = @fd = null

# ### Tier File Storage
#
# Tiers are identified by a number tier address. The tier address is a number
# that is incremented as new tier are created. A branch tier file has a file
# name that includes the tier address. The file name is derived from the tier
# address. When we load a tier, we first derive the file name from the tier
# address, then we load the file.
#
# The leaf tier file contains insert objects, delete objects and address array
# objects, stored as JSON, one object per line, as described above.
#
# The branch tier file contains a single object that includes the array of
# tier addresses. The tier addresses reference leaf tier files. The tier
# addresses are sorted according to the value of the objects in leaf tiers they
# reference. The addresses are sorted by the tree collation using the record
# from the referenced leaf tier that has the least value according to the tree
# collation.
#
# Put another way, each branch element in a branch tier represents a leaf tier.
# The leaf tiers are sorted by the tree collation. The object value used to sort
# the branch tier is first element in the sorted leaf tier.
#
# TODO Okay, what? I'm forgetting that I need the addresses to inner tiers as
# well as to leaf tiers, both, when the tier is not penultimate. Descending is
# not going to work, that is going to load the entire tree into memory quickly.
#
# Hmm... Maybe not. Tree four deep means loading only a particular path. It
# ensures that we read lock our way down the tree to get our record. Although, I
# believe I thought long and hard about this, and jumping down to the leaves
# wasn't a problem.
#
# We only need to store the sorted array of the leaf tier addresses in our
# branch tier, since we can obtain the object used to collate the address

#
class IO
  # Set directory and extractor. Initialze cache and MRU list.
  constructor: (@directory, @extractor) ->
    @cache          = {}
    @head           = { address: -1 }
    @head.next      = @head
    @head.previous  = @head
    @nextAddress    = 0

  # Create the database detroying any exisiting database.

  # &mdash;
  create: (_) ->
    # Create the directory if it doesn't exist.
    try
      stat = fs.stat @directory, _
      if not stat.isDirectory()
        throw new Error "database #{@directory} is not a directory."
    catch e
      throw e if e.code isnt "ENOENT"
      fs.mkdir @directory, 0755, _
    # Create a root branch with a single empty leaf.
    root = @allocateBranches true
    leaf = @allocateLeaves([], -1)
    root.addresses.push leaf.address
    # Write the root branch.
    @writeBranches root, "", _
    @rewriteLeaves leaf, _
    @relink leaf, _

  # Open an existing database.
  
  # &mdash;
  open: (_) ->
    try
      stat = fs.stat @directory, _
      if not stat.isDirectory()
        throw new Error "database #{@directory} is not a directory."
    catch e
      if e.code isnt "ENOENT"
        throw new Error "database #{@directory} does not exist."
      else
        throw e
    for file in fs.readdir @directory, _
      if match = /^segment(\d+)$/.exec file
        address = parseInt match[1], 10
        @nextAddress = address + 1 if address > @nextAddress

  # TODO Very soon.
  close: (_) ->

  # Link tier to the head of the MRU list.
  link: (entry) ->
    next = @head.next
    entry.next = next
    next.previous = entry
    @head.next = entry
    entry.previous = @head
    entry

  # Unlnk a tier from the MRU list.
  unlink: (entry) ->
    { next, previous } = entry
    next.previous = previous
    previous.next = next
    entry

  # Generate a file name for a tier file with an optional suffix for temporary
  # rewrite files. Tier file names are generated from the tier address.
  filename: (address, suffix) ->
    suffix or= ""
    padding = "00000000".substring(0, 8 - String(address).length)
    "#{@directory}/segment#{padding}#{address}#{suffix}"

  allocateBranches: (penultimate) ->
    address = @nextAddress++
    tier = @link({ penultimate, address, addresses: [], cache: {} })
    @cache[address] = tier

  allocateLeaves: (addresses, right) ->
    address = @nextAddress++
    @cache[address] = @link
      leaf: true
      address: address
      addresses: addresses
      cache: {}
      right: right
      io: new LeafIO @filename address

  # Going forward, every will story keys, so that when we encounter a record, we
  # don't have to run the extractor, plus we get some caching.
  #
  # Or maybe just three arrays, key, object and address? I'm talking about using
  # each key as an object cache, by adding an object member to the key. But,
  # they cache container is a hash table, which is a compactish representation,
  # so why not make it yet anohter hash table? The logic gets a lot easier.
  object: (tier, index, _) ->
    address = tier.addresses[index]
    if not object = tier.cache[address]
      object = tier.cache[address] = tier.io.readObject address, _
    object

  key: (tierOrAddress, index, _) ->
    if typeof tierOrAddress is "number"
      leaf  = @lock false, tierOrAddress, true, _
      key = @extractor @object leaf, index, _
      @release leaf
      key
    else if tierOrAddress.leaf
      @extractor @object tierOrAddress, index, _
    else
      address = tierOrAddress.addresses[index]
      if not key = tierOrAddress.cache[address]
        # Here's something to consdier. The only time we lock the leaf is during
        # descent, to read in the key.
        #
        # If this leaf tier is locked exclusively, we are not the ones holding the
        # lock, because that would mean that we are locking the inner tier, we
        # would already have traversed the inner tier, the key would be in the
        # cache.
        #
        # If this leaf tier has an exclusive lock pending, it is not a lock that
        # we are waiting for, so we can append a shared lock and wait for multiple
        # exclusive and shared locks to clear.
        key = tierOrAddress.cache[address] = @key address, 0, _
      key

  # Write the branches to a file as a JSON string. We tuck the tier properties
  # into an object before we serialize it, so that it is easy deserialize.
  writeBranches: (tier, suffix, _) ->
    filename = @filename tier.address, suffix
    record = [ tier.penultimate, tier.next?.address, tier.addresses ]
    json = JSON.stringify(record) + "\n"
    fs.writeFile filename, json, "utf8", _

  # Read branches from a brach tier file and assign the tier properties to the
  # given tier object.
  #
  # TODO: Move link to head always, to maintain MRU.
  readBranches: (tier, _) ->
    filename = @filename tier.address
    json = fs.readFile filename, "utf8", _
    [ penultimate, next, addresses ] = JSON.parse json
    extend tier, { penultimate, next, addresses }

  # TODO Consider: can't I always just rewrite and link?
  rewriteBranches: (tier, _) ->
    @writeBranches(tier, ".new", _)

  # TODO Ensure that you close the current tier I/O. Also, you must also be very
  # much locked before you use this, but I'm sure you know that.
  
  # Compact a leaf tier file by writing it to a temporary file that will become
  # the permanent file. All of records referenced by the current address array a
  # appended to a new leaf tier file using insert objects. An address array
  # object is appended to the end of new leaf tier file. The new file will load
  # quickly, because the address array object will be found immediately.
  rewriteLeaves: (tier, _) ->
    io = new LeafIO @filename(tier.address, ".new")
    addresses = []
    cache = {}
    for i in [0...tier.addresses.length]
      object = @object tier, i, _
      address = io.writeInsert(object, i, _)
      addresses.push address
      cache[address] = object
    io.writeAddresses(tier.right, addresses, _)
    io.close(_)
    extend tier, { addresses, cache }

  readLeaves: (tier, _) ->
    filename = @filename tier.address
    stat = fs.stat filename, _
    fd = fs.open filename, "r+", _

    # Obviously, while all this is going on, I could end up writing to the
    # leaf, because it is not actually locked. First lock, then load. The
    # entry object needs to be linked, then loaded.
    line      = ""
    offset    = -1
    end       = stat.size
    eol       = stat.size
    splices   = []
    addresses = []
    cache     = {}
    buffer    = new Buffer(1024)
    while end
      end     = eol
      start   = end - buffer.length
      start   = 0 if start < 0
      read    = fs.read fd, buffer, 0, buffer.length, start, _
      end    -= read
      offset  = read
      if buffer[--read] isnt 0x0A
        throw new Error "corrupt leaves"
      eos     = read + 1
      stop    = if start is 0 then 0 else -1
      while read != 0
        read = read - 1
        if buffer[read] is 0x0A or read is stop
          record = JSON.parse buffer.toString("utf8", read, eos)
          eos   = read + 1
          index = record.shift()
          if index is 0
            [ addresses, end ] = [ record, 0 ]
            break
          else
            splices.push [ index, start + read ]
            if index > 0
              cache[start + read] = record.shift()
      eol = start + eos
    splices.reverse()
    for splice in splices
      [ index, address ] = splice
      if index > 0
        addresses.splice(index - 1, 0, address)
      else
        addresses.splice(-(index + 1), 1)
    fs.close fd, _
    loaded = true
    io = new LeafIO @filename tier.address
    tier = extend tier, { addresses, cache, loaded, io }

  # Move a new branch tier file into place. Unlink the existing branch tier
  # file, then rename the new branch tier file to the permanent name of the
  # branch tier file.
  relink: (tier, _) ->
    replacement = @filename(tier.address, ".new")
    stat = fs.stat replacement, _
    if not stat.isFile()
      throw new Error "not a file"
    permanent = @filename(tier.address)
    try
      fs.unlink permanent, _
    catch e
      throw e unless e.code is "ENOENT"
    fs.rename replacement, permanent, _

  # TODO Move down, bring read and write up.
  lock: (exclusive, address, leaf, callback) ->
    if not tier = @cache[address]
      @cache[address] = tier =
        @link({ leaf, address, cache: {}, addresses: [], locks: [[]] })
    locks = tier.locks
    if tier.loaded
      lock = callback
    else
      lock = (error, tier) =>
        if error
          callback error
        else if tier.loaded
          callback null, tier
        else if leaf
          @readLeaves tier, callback
        else
          @readBranches tier, callback
    if exclusive
      throw new Error "already locked" unless locks.length % 2
      locks.push [ lock ]
      locks.push []
      if locks[0].length is 0
        locks.shift()
        lock(null, tier)
    else
      locks[locks.length - 1].push lock
      if locks.length is 1
        lock(null, tier)

  resume: (tier, continuations) ->
    process.nextTick =>
      for callback in continuations
        callback(null, tier)

  # Remove the lock callback from the callback list..
  release: (tier) ->
    locks = tier.locks
    running = locks[0]
    running.shift()
    say { locks }
    if running.length is 0 and locks.length isnt 1
      locks.shift()
      @resume tier, locks[0] if locks[0].length
      
  upgrade: (tier, _) ->
    @release tier
    @lock true, tier.address, false, _

class exports.Strata
  # Construct the Strata from the options.
  constructor: (options) ->
    defaults =
      leafSize: 12
      branchSize: 12
      comparator: comparator
      extractor: extractor
    @_options       = extend defaults, options
    @_io            = new IO options.directory, @_options.extractor

  create: (_) -> @_io.create(_)

  open: (_) -> @_io.open(_)

  close: (_) -> @_io.close(_)

  get: (key, callback) ->
    operation = method: "get"
    mutation = new Mutation(@, null, key, operation)
    mutation.mutate callback

  insert: (object, callback) ->
    operation = method: "insertSorted"
    mutation = new Mutation(@, object, null, operation)
    mutation.mutate callback

  # Both `insert` and `remove` use this generalized mutation method that
  # implements locking the proper tiers during the descent of the tree to find
  # the leaf to mutate.
  #
  # This generalized mutation will insert or remove a single item.
  _generalized: (mutation, _) ->
    mutation.descend false

    tier = mutation.parent.tier = @_lock mutation.parent, 0, false, _

    # Perform the root decision with only a read lock obtained.
    mutation.decisions.initial.call @, mutation, _

    # Determine if we need to lock a tier. We have different criteria for inner
    # tiers with inner tier children then for penultimate inner tiers with leaf
    # tier children.
    loop
      { decisions, parent } = mutation
      break if parent.tier.penultimate
      @_testInnerTier mutation, decisions.subsequent, "swap", _
      branch = @_find(mutation.parent.tier, mutation.sought)
      mutation.descend false
      branch.child.tier = @_load branch.address, false, _

    # Make decisions based on the penultimate level.
    @_testInnerTier mutation, decisions.penultimate, "", _

    # All the necessary locks have been obtained and we are able to mutate the
    # tree with impunity.

    # Run through levels, bottom to top, performing the operations at for level.
    mutation.levels.reverse()
    mutation.levelQueue = mutation.levels.slice(0)
    operation = mutation.leafOperation
    operation.shift().apply @, operation.concat mutation, @_leafDirty
      
  # Perform decisions related to the inner tier.
  _testInnerTier: (mutation, decision, exclude, _) ->
    mutation.decisions.swap.call @, mutation
    # If no split/merge, then clear all actions except for swap.
    if not decision.call @, mutation, _
      for step in mutation.plan
        step.operations = step.operations.filter (operation) ->
          operation isnt exclude

  # The leaf operation may or may not alter the leaf. If it doesn't, all of the
  # operations to split or merge the inner tiers are moot, because we didn't
  # make the changes to the leaf that we expected.
  _leafDirty: (mutation) ->
    if mutation.dirtied
      @_operateOnLevel mutation
    else
      @_unlock mutation

  _operateOnLevel: (mutation) ->
    if mutation.levelQueue.length is 0
      @_unlock mutation
    else
      @_operateOnOperation mutation

  _operateOnOperation: (mutation) ->
    if mutation.levelQueue[0].operations.length is 0
      mutation.levelQueue.shift()
      @_operateOnLevel mutation
    else
      operation = mutation.levelQueue[0].operations.pop()
      operation.shift().apply @, operation.concat mutation, @_operateOnOperation

  # Inovke the blocker method, used for testing locks, if it exists, otherwise
  # release all locks.
  _unlock: (mutation) ->
    if mutation.blocker
      mutation.blocker => @_releaseLocks(mutation)
    else
      @_releaseLocks(mutation)
      
  # When we release the locks, we send the first waiting operations we encounter
  # forward on the next tick. We don't have to keep track of this, because the
  # first waiting operations will always be in top most level. 
  _releaseLocks: (mutation) ->
    # For each level locked by the mutation.
    for level in mutation.levels
      level.release(@)

    # We can tell the user that the operation succeeded on the next tick.
    process.nextTick -> mutation.callback.call null, mutation.dirtied

  _resume: (continuations) ->

  _record: (tier, index, _) ->
    if tier.leaf
      process.exit 1
    else
      process.exit 1

  # If the root is full, we add a root split operation to the operation stack.
  _shouldDrainRoot: (mutation, _) ->
    if @innerSize is mutation.parent.tier.objects.length
      mutation.plan[0].push
        operation: "_splitRoot",
        addresses: [ mutation.parent.tier.address ]

  # To split the root, we copy the contents of the root into two children,
  # splitting the contents between the children, then make the two children the
  # only two nodes of the root tier. The address of the root tier does not
  # change, only the contents.
  #
  # While a split at lower levels will create two half empty tiers and add a
  # single branch to the parent, this operation will empty the root into two
  # separate tiers, creating an almost empty root each time it is split.
  _drainRoot: (mutation) ->
    # Create new left and right inner tiers.
    left = @_newInnerTier mutation, root.penultimate
    right = @_newInnerTier mutation, root.penultimate

    # Find the partition index and move the branches up to the partition
    # into the left inner tier. Move the branches at and after the partiion
    # into the right inner tier.
    partition = root.lenth / 2
    fullSize = root.length
    for i in [0...partition]
      left.push root[i]
    for i in [partition...root.length]
      right.push root[i]

    # Empty the root.
    root.length = 0

    # The left-most pivot or the right inner tier is null.
    pivot = right[0].record
    right[0].record = null

    # Add the branches to the new left and right inner tiers to the now
    # empty root tier.
    root.push { pivot: null, address: left.address }
    root.push { pivot, address: right.address }

    # Set the child type of the root tier to inner.
    root.penultimate = false

    # Stage the dirty tiers for write.
    @io.dirty root, left, right

  # Determine if the root inner tier should be filled with contents of a two
  # extra remaining inner tier children. When an root inner tier has only one
  # inner tier child, the contents of that inner tier child becomes the root of
  # the b-tree.
  _shouldFillRoot: (mutation) ->
    if not root.penultimate and root.length is 2
      first = mutation.io.load root[0].childAddress
      second = mutation.io.load root[1].childAddress
      if first.length + second.length is @innerSize
        mutations.parentLevel.operations.add @_fillRoot
        return true
    false

  # Determines whether to merge two inner tiers into one tier or else to delete
  # an inner tier that has only one child tier but is either the only child tier
  # or its siblings are already full.
  #
  # **Only Children**
  #
  # It is possible that an inner tier may have only one child leaf or inner
  # tier. This occurs in the case where the siblings of of inner tier are at
  # capacity. A merge occurs when two children are combined. The nodes from the
  # child to the right are combined with the nodes from the child to the left.
  # The parent branch that referenced the right child is deleted.
  # 
  # If it is the case that a tier is next to full siblings, as leaves are
  # deleted from that tier, it will not be a candidate to merge with a sibling
  # until it reaches a size of one. At that point, it could merge with a sibling
  # if a deletion were to cause its size to reach zero.
  #
  # However, the single child of that tier has no siblings with which it can
  # merge. A tier with a single child cannot reach a size of zero by merging.
  #
  # If were where to drain the subtree of an inner tier with a single child of
  # every leaf, we would merge its leaf tiers and merge its inner tiers until we
  # had subtree that consisted solely of inner tiers with one child and a leaf
  # with one item. At that point, when we delete the last item, we need to
  # delete the chain of tiers with only children.
  #
  # We deleting any child that is size of one that cannot merge with a sibling.
  # Deletion means freeing the child and removing the branch that references it.
  #
  # The only leaf child will not have a sibling with which it can merge,
  # however. We won't be able to copy leaf items from a right leaf to a left
  # leaf. This means we won't be able to update the linked list of leaves,
  # unless we go to the left of the only child. But, going to the left of the
  # only child requires knowing that we must go to the left.
  #
  # We are not going to know which left to take the first time down, though. The
  # actual pivot is not based on the number of children. It might be above the
  # point where the list of only children begins. As always, it is a pivot whose
  # value matches the first item in the leaf, in this case the only item in the
  # leaf.
  # 
  # Here's how it works.
  #
  # On the way down, we look for a branch that has an inner tier that is size of
  # one. If so, we set a flag in the mutator to note that we are now deleting.
  #
  # If we encounter an inner tier has more than one child on the way down we are
  # not longer in the deleting state.
  #
  # When we reach the leaf, if it has a size of one and we are in the deleting
  # state, then we look in the mutator for a left leaf variable and an is left
  # most flag. More on those later as neither are set.
  #
  # We tell the mutator that we have a last item and that the action has failed,
  # by setting the fail action. Failure means we try again.
  #
  # On the retry, as we descend the tree, we have the last item variable set in
  # the mutator.
  #
  # Note that we are descending the tree again. Because we are a concurrent data
  # structure, the structure of the tree may change. I'll get to that. For now,
  # let's assume that it has not changed.
  #
  # If it has not changed, then we are going to encounter a pivot that has our
  # last item. When we encounter this pivot, we are going to go left. Going left
  # means that we descend to the child of the branch before the branch of the
  # pivot. We then follow each rightmost branch of each inner tier until we reach
  # the right most leaf. That leaf is the leaf before the leaf that is about to
  # be removed. We store this in the mutator.
  #
  # Of course, the leaf to be removed may be the left most leaf in the entire
  # data structure. In that case, we set a variable named left most in the
  # mutator.
  #
  # When we go left, we lock every inner tier and the leaf tier exclusive, to
  # prevent it from being changed by another query in another thread. We always
  # lock from left to right.
  #
  # Now we continue our descent. Eventually, we reach out chain of inner tiers
  # with only one child. That chain may only be one level deep, but there will be
  # such a chain.
  #
  # Now we can add a remove leaf operation to the list of operations in the
  # parent level. This operation will link the next leaf of the left leaf to the
  # next leaf of the remove leaf, reserving our linked list of leaves. It will
  # take place after the normal remove operation, so that if the remove operation
  # fails (because the item to remove does not actually exist) then the leave
  # removal does not occur.
  #
  # I revisited this logic after a year and it took me a while to convince myself
  # that it was not a misunderstanding on my earlier self's part, that these
  # linked lists of otherwise empty tiers are a natural occurrence.
  #
  # The could be addressed by linking the inner tiers and thinking harder, but
  # that would increase the size of the project.
  _shouldMergeInner: (mutation) ->
    # Find the child tier.
    branch = @_find parent, mutation.fields
    child = @_pool.load parent[branch].childAddress

    # If we are on our way down to remove the last item of a leaf tier that is
    # an only child, then we need to find the leaf to the left of the only child
    # leaf tier. This means that we need to detect the branch that uses the the
    # value of the last item in the only child leaf as a pivot. When we detect
    # it we then navigate each right most branch of the tier referenced by the
    # branch before it to find the leaf to the left of the only child leaf. We
    # then make note of it so we can link it around the only child that is go be
    # removed.
    lockLeft = mutation.onlyChild and pivot? and not mutation.leftLeaf?
    if lockLeft
      lockLeft = @comparison(mutation.fields, @io.fields pivot) is 0
    if lockLeft
      # FIXME You need to hold these exclusive locks, so add an operation that
      # is uncancelable, but does nothing.
      index = parent.getIndexOfChildAddress(child.getAddress()) - 1
      inner = parent
      while not inner.childLeaf
        inner = pool.load(mutation.getStash(), inner.getChildAddress(index))
        levelOfParent.lockAndAdd(inner)
        index = inner.getSize() - 1
      leaf = pool.load(mutation.getStash(), inner.getChildAddress(index))
      levelOfParent.lockAndAdd(leaf)
      mutation.setLeftLeaf(leaf)

    # When we detect an inner tier with an only child, we note that we have
    # begun to descend a list of tiers with only one child.  Tiers with only one
    # child are deleted rather than merged. If we encounter a tier with children
    # with siblings, we are no longer deleting.
    if child.length is 1
      mutation.deleting = true
      levelOfParent.operations.push @_removeInner(parent, child)
      return true

    # Determine if we can merge with either sibling.
    listToMerge = []

    index = parent.getIndexOfChildAddress(child.getAddress())
    if index != 0
      left = pool.load(mutation.getStash(), parent.getChildAddress(index - 1))
      levelOfChild.lockAndAdd(left)
      levelOfChild.lockAndAdd(child)
      if left.getSize() + child.getSize() <= structure.getInnerSize()
        listToMerge.add(left)
        listToMerge.add(child)

    if index is 0
      levelOfChild.lockAndAdd(child)

    if listToMerge.isEmpty() && index != parent.getSize() - 1
      right = pool.load(mutation.getStash(), parent.getChildAddress(index + 1))
      levelOfChild.lockAndAdd(right)
      if (child.getSize() + right.getSize() - 1) == structure.getInnerSize()
        listToMerge.add(child)
        listToMerge.add(right)

    # Add the merge operation.
    if listToMerge.size() != 0
      # If the parent or ancestors have only children and we are creating
      # a chain of delete operations, we have to cancel those delete
      # operations. We cannot delete an inner tier as the result of a
      # merge, we have to allow this subtree of nearly empty tiers to
      # exist. We rewind all the operations above us, but we leave the
      # top two tiers locked exclusively.

      # FIXME I'm not sure that rewind is going to remove all the
      # operations. The number here indicates that two levels are
      # supposed to be left locked exclusive, but I don't see in rewind,
      # how the operations are removed.
      if mutation.deleting
        mutation.rewind 2
        mutation.deleting = false

      levelOfParent.operations.push new MergeInner(parent, listToMerge.get(0), listToMerge.get(1))

      return true

    # When we encounter an inner tier without an only child, then we are no
    # longer deleting. Returning false will cause the Query to rewind the
    # exclusive locks and cancel the delete operations, so the delete
    # action is reset.
    mutation.deleting = false

    return false
 
  _shouldSplitInner: (mutation) ->
    console.log "_shouldSplitInner"
    structure = mutation.getStructure()
    branch = parent.find(mutation.getComparable())
    child = structure.getStorage().load(mutation.getStash(), parent.getChildAddress(branch))
    levelOfChild.lockAndAdd(child)
    if child.getSize() == structure.getInnerSize()
      levelOfParent.operations.add(new SplitInner(parent, child))
      return true
    return false

  _never: -> false

  # Determine if the leaf that will hold the inserted value is full and ready
  # to split, if it is full and part of linked list of b+tree leaves of
  # duplicate index values, or it it can be inserted without splitting.
  #
  # TODO Now there is a chance that this might already be in a split state. What
  # do we do? Do we create a new plan as we decend to test the current plan?
  _howToInsertLeaf: (mutation, _) ->
    # Find the branch that navigates to the leaf child.
    branch = @_find mutation.parent.tier, mutation.fields, _
    address = mutation.parent.tier.addresses[branch]
    
    # Lock the leaf exclusively.
    mutation.child.exclusive = true
    leaf = @_lock mutation.child, address, true, _
    mutation.child.tier = leaf

    # If the leaf size is equal to the maximum leaf size, then we either
    # have a leaf that must split or a leaf that is full of objects that
    # have the same index value. Otherwise, we have a leaf that has a free
    # slot.
    if leaf.addresses.length is @leafSize
      # If the index value of the first value is equal to the index value
      # of the last value, then we have a linked list of duplicate index
      # values. Otherwise, we have a full page that can split.
      first = @extractor leaf.record(0)
      if @compartor(first, @extractor leaf.record(leaf.size() - 1)) is 0
        # If the inserted value is less than the current value, create
        # a new page to the left of the leaf, if it is greater create a
        # new page to the right of the leaf. If it is equal, append the
        # leaf to the linked list of duplicate index values.
        compare = @comparator mutation.fields, first
        # TODO We will never split left! The inserted value is never less than
        # the first value in the leaf! Assert this and get rid of the left
        # split.
        if compare < 0
          mutation.leafOperation = [ @_splitLinkedListLeft, parent ]
        else if compare > 0
          mutation.leafOperation = [ @_splitLinkedListRight, parent ]
        else
          mutation.leafOperation = [ @_insertLinkedList, leaf ]
          split = false
      else
        # Insert the value and then split the leaf.
        parentLevel.operations.push [ @_splitLeaf, parent ]
        mutation.leafOperation = [ @_insertSorted, parent ]
    else
      # No split and the value is inserted into leaf.
      mutation.leafOperation = [ @_insertSorted, leaf ]
      split = false

    # Let the caller know if we've added a split operation.
    split

# Mark the operations that split or merge the tree. These are the operations we
# cancel if split or merge is not necessary, because a decendent tier will not
# split or merge.
# 
# That is, splits and merges ripple up from the leaves, so if
# the root, say is empty and ready to merge, we will lock it to merge it, but if
# it has an inner tier child that is not ready to merge, we remove the root
# merge operation. If there are no other operations in the tier we can unlock it.
#
# Swap operations are not cancelable.
for operation in "_drainRoot".split /\n/
  module.exports.Strata.prototype[operation].isSplitOrMerge = true
