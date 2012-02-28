# A Streamline.js friendly evented I/O b&#x2011;tree for Node.js.
#
# ## Purpose
#
# Strata stores JSON objects on disk, according to a sort order of your
# choosing, indexed for fast retrieval.
#
# Strata is faster than a flat file, lighter than a database, with more capacity
# than an in&#x2011;memory tree.
# 
# Strata is a [b&#x2011;tree](http://en.wikipedia.org/wiki/B-tree)
# implementation for [Node.js](http://nodejs.org/) that is **evented**,
# **concurrent**, **persistent** and **durable**.
#
# Strata is **evented**. It uses asynchronous I/O to read and write
# b&#x2011;tree pages, allowing your CPU to continue to do work while Strata
# waits on I/O.
#
# Strata is **concurrent**. Strata will answer any queries from its
# in&#x2011;memory cache when it can, so requests can be satisifed even when
# there are evented I/O requests outstanding.
#
# Strata is **persistent**. It stores your tree in page files. The page files
# are plain old JSON, text files that are easy to manage. You can view them with
# `less` or `tail -f`, back them up hot using `rsync`, and version them with
# `git`.
#
# Strata is **durable**. It only appends records to to file, so a hard shutdown
# will only ever lose the few last records added. Pages are journaled when they
# are vacuumed or rewritten. 
#
# Strata is a b&#x2011;tree. A b&#x2011;tree is a database primiative. Using
# Strata, you can start to experiment with database designs of your own. You can
# use Strata to build an MVCC database table, like PostgreSQL. You can create
# Strata b&#x2011;trees to create indexes into structured data that is not
# already in a database, like monster log files. You can use Strata to store
# your data in what ever form of JSON suits you like a NoSQL database.
#
# As a bonus, Strata is two database primatives in one, because with a time
# series index, you can use Strata as a write&#x2011;ahead log.
#
# Strata runs anywhere that Node.js runs, in pure JavaScript.
#
# ## Collaboration
# 
# Documentation for Strata is presented here, in literate programming style. API
# documentation is not yet available, so scoot down to the `Strata` class for an
# in-depth look at the API. Have a look at the extensive test suite for examples
# on the different operations.
#
# If you find a bug with Strata, please report it at the [GitHub
# Issues](https://github.com/bigeasy/strata/issues). If you want to leave a
# comment on the documentation or the code, you can ping me, Alan Gutierrez, at
# @[bigeasy](https://twitter.com/#!/bigeasy) on Twitter.
#
# Feel free to fork and explore. Note that Strata is a database primitive, not a
# database in itself. Before you fork and add what you feel are missing
# features, please consult with me. Perhaps your ideas are better expressed as
# project that employs Strata, intead of to a patch to Strata itself.
#
# ## Terminology
#
# We refer to the nodes in our b&#x2011;tree as ***pages***. The term node
# conjures an image of a discrete component in a linked data structure that
# contains one, maybe two or three, values. Nodes in a b&#x2011;tree contain
# hundreds or thousands of values. They are indexed. They are read from disk.
# They are allowed to fall out of memory when they have not been recently
# referenced. These are behaviors that people associate with a page of values.
#
# Otherwise, we use common terminology for ***height***, ***depth***,
# ***parent***, ***child***, ***split*** and ***merge***.
#
# There is no hard and fast definition for all the terms. A leaf is a fuzzy
# concept in b&#x2011;tree literature, for example. We call a page that contains
# records a ***leaf page***. We call a non-leaf page a ***branch page***. The
# term ***order*** means different things to different people.  We define the
# order of a branch page to be the maximum number of child pages, while the
# order of a leaf page to be the maximum number of records.
#
# The term ***b&#x2011;tree*** itself may not be correct. There are different
# names for b&#x2011;tree that reflect the variations of implementation, but
# those distinctions have blurred over the years. Our implemenation may be
# considered a b+tree, since pages are linked, and records are stored only in
# the leaves.
#
# Terms specific to our implementation will be introduced as they are
# encountered in the document. 
#
# ## What is a b&#x2011;tree?
#
# This documentation assumes that you understand the theory behind the
# b&#x2011;tree, and know the variations of implementation. If you are
# interested in learning about b&#x2011;trees you should start with the
# Wikipedia articles on [B-trees](http://en.wikipedia.org/wiki/B-tree) and
# [B+trees](http://en.wikipedia.org/wiki/B%2B_tree). I was introduced to
# b&#x2011;trees while reading [Algorithms in
# C](http://www.amazon.com/dp/0201314525), quite some time ago.
#
# ## What flavor of b&#x2011;tree is this?
#
# Strata is a b&#x2011;tree with leaf pages that contain records ordered by the
# collation order of the tree. Records are stored for retrieval in constant
# time, addressed by an integer index, so that they can be found using binary
# search.
#
# Branch pages contain links to other pages, and do not store records
# themselves.
#
# Leaf pages are linked in ascending order to simply the implementatoin of
# traversal by cursors. Branch pages are singly linked in ascending order to
# simplify implementation of branch page merges.
#
# The order of a branch page is the maximum number of children for a branch
# page. The order of a leaf page is maximum number of records for a leaf page.
# When a page exceeds its order it is split into two or more pages. When two
# sibling pages next to each other can be combined to create a page less than
# than the order they are merged.
#
# The b&#x2011;tree always has a root branch page. The height of the tree
# increases when the root branch page is split. It decreases when the root
# branch page is merged. The split of the root branch is a different operation
# from the split of a non-root branch, because the root branch does not have
# siblings.

# Requried node.js libraries.
fs = require "fs"

# Copy values from one hash into another.
extend = (to, from) ->
  to[key] = value for key, value of from
  to

# Used for debugging. If you don't see them called in the code, it means the
# code is absolutely bug free.
die = (splat...) ->
  console.log.apply null, splat if splat.length
  process.exit 1
say = (splat...) -> console.log.apply null, splat

# ## Collation
#
# A b&#x2011;tree has a collation defined by the application developer.
#
# The collation is determined by the combination of an extractor and a
# comparator. The extractor is used to extract a key from the stored record. The
# comparator is used to order records by comparing the key.
#
# Separating extraction from comparison allows us to cache the key. We do not
# need the whole record for comparison, only the key. Keys are used to order the
# tree, so when we will constantly be reading records off the disk solely to get
# their key value.
#
# If a record is read for its key, but the record is not soon visited by a
# cursor, it will eventually be collected by a cache purge, If the key is
# frequently consulted by queries as they descend the tree, the key will be
# retained. If the key is subset of a large record, purging the records and
# retaining the keys will reduce the in&#x2011;memory size of the b&#x2011;tree.
#
# Also, the comparator is pretty easily generalized, while the exractor is
# invariably specialized. You might have a single string comparator that you use
# with extractors specialized for different types of records.
#
# ### Default Collation
#
# You will almost certainly define your down extractors and comparators, but the
# b&#x2011;tree has a default that works for b&#x2011;tree that stores only
# JavaScript primitives.

# Default comparator is good only for strings, use a - b for numbers.
comparator = (a, b) ->
  if a < b then -1 else if a > b then 1 else 0

# Default extractor returns the value as whole, i.e. a b&#x2011;tree of strings.
extractor = (a) -> a

# ## Pages
#
# Our b&#x2011;tree has two types of pages. Leaf pages and branch pages.
#
# A ***leaf page*** contains records. A ***branch page*** contains references to
# other pages.
#
# Both leaf pages and branch pages are ordered according to the collation.
#
# To find a record, we descend a tree of branch pages to find the leaf page that
# contains the record. That is a b&#x2011;tree.
#
# ## Page Storage
#
#
# The `IO` class manages the reading and writing of leaf and branch pages to and
# from disk, page locking and page caching. It also implements the binary search
# we use to search the pages.

#
class IO
  # Each page is stored in its own ***page file***. The page files are all kept
  # in a single directory. The directory is specified by the application
  # developer when the `Strata` object is constructed.
  #
  # Page files contain one or more JSON strings, one string per line. The line
  # based JSON format plays nice with traditional UNIX text utilities.
  #
  # A ***leaf page file*** contains ***insert objects***, ***delete objects***
  # and ***position array objects***, stored as JSON, one object per line, as
  # described above. The JSON objects stored on behalf of the client are called
  # ***records*** and they are contained within the insert objects.
  #
  # A ***branch page file*** contains a single JSON object stored on a single
  # line that contains the array of child page addresses.
  #
  # When we read records and record keys off the disk, we store them in an
  # object that acts as a cache for the page. The in memory page object contains
  # an array of integers that act as either page addresses or record positions.
  # We call this the ***reference array***. The integers are stored in the
  # reference array in the collation order of the stored records they reference.
  #
  # The in memory page object also contains a map of integer addresses to JSON
  # objects. This is the ***record cache*** for the page object. The integers in
  # the reference array are always present when the page is loaded, so that the
  # integer array is dense, but the record cache only contains entries for
  # records that have been referenced. We use a binary search to probe for keys
  # and records, so we can avoid loading records we don't need.
  # 
  # We count on our JavaScript array implemenation to be [clever about memory
  # usage](http://stackoverflow.com/questions/614126/why-is-array-push-sometimes-faster-than-arrayn-value/614255\#614255).
   
  # Set directory and extractor. Initialze the page cache and most-recently used
  # list.
  constructor: (@directory, @options) ->
    @cache          = {}
    @mru            = { address: null }
    @mru.next       = @mru
    @mru.previous   = @mru
    @nextAddress    = 0
    @length         = 1024
    @balancer       = new Balancer
    @size           = 0
    { @extractor
    , @comparator } = @options

  # Pages are identified by an integer page address. The page address is a number
  # that is incremented as new pages are created. A page file has a file name that
  # includes the page address.  When we load a page, we first derive the file name
  # from the page address, then we load the file.
  #
  # TK Rewrite once we've finalized journaled balancing.
  #
  # The `filename` method accepts a suffix, so that we can create replacement
  # files. Instead of overwriting an existing page file, we create a replacement
  # with the suffix `.new`. We then delete the existing file with the `delete`
  # method and move the replacement into place with the `replace` method. This
  # two step write is part of our crash recovery strategy.
  #
  # We always write out entire branch page files. Leaf pages files are updated
  # by appending, but on occasion we rewrite them to vaccum deleted records.

  # Create a file name for a given address with an optional suffix.
  filename: (address, suffix) ->
    address = Math.abs(address)
    suffix or= ""
    padding = "00000000".substring(0, 8 - String(address).length)
    "#{@directory}/segment#{padding}#{address}#{suffix}"

  # TODO I thought we broke this up?

  # Move a replacement page file into place. Unlink the existing page file, then
  # rename the new page file to the permanent name of the page file.
  relink: (page, _) ->
    replacement = @filename(page.address, ".new")
    stat = fs.stat replacement, _
    if not stat.isFile()
      throw new Error "not a file"
    permanent = @filename(page.address)
    try
      fs.unlink permanent, _
    catch e
      throw e unless e.code is "ENOENT"
    fs.rename replacement, permanent, _

  # ### Page Caching
  #
  # We keep an in&#x2011;memory map of page addresses to page objects. This is
  # our ***page cache***.
  #
  # #### Most-Recently Used List
  #
  # We also maintain a ***most-recently used list*** as a linked list using the
  # page objects as list nodes. When we reference a page, we unlink it from the
  # linked list and relink it at the head of the ist. When we want to cull the
  # cache, we can remove the pages at the end of the linked list, since they are
  # the least recently used.
  #
  # #### Cache Purge Trigger
  #
  # There are a few ways we could schedule a cache purge; elapsed time, after a
  # certain number of requests, when a reference count reaches zero, or when
  # when a limit is reached.
  #
  # We take the limits approach. The bluk of a cached page is the size of the
  # references array and the size of objects in records map. We keep track of
  # those sizes. When we reach an application developer specified maxmimum size
  # for cached records and page references for the entire b&#x2011;tree, we
  # trigger a cache purge to bring it below the maxiumum size. The purge will
  # remove entries from the end of the most-recently used list until the limit
  # is met.
  #
  # #### Pages Held for Housekeeping
  #
  # There may be cache entires loaded for housekeeping only. When balancing the
  # tree, the page item count is needed to determine if needs to be split, or if
  # it can merged with a sibling page.
  #
  # We only need the *item count* to create our balance plan, however, not the
  # cached references and records. These cache entries can be purged of cached
  # records and page references, but the entry itself cannot be deleted until it
  # is no longer needed to calculate a merge.
  #
  # We use reference counting to determine if an entry is participating in
  # balance calcuations. We can purge the cached records, but we do not unlink
  # the page object from the most-recenlty used list nor remove it from the
  # cache.
  #
  # #### JSON Size
  #
  # Limits would be difficult to guage if we out b&#x2011;tree were an
  # in&#x2011;memory data structure, but we can get an accuate relative measure
  # of the size of a page using the length of the JSON strings used to store
  # records and references. 
  #
  # The JSON size of a branch page is the string length of the JSON serialized
  # page address array. The JSON size of leaf page is the string length of the
  # file position array when serialized with JSON, plus the string length of
  # each record loaded in memory when JSON serialized with JSON.
  #
  # This is not an exact measure of the system memory committed to the in memory
  # representation of the b&#x2011;tree. It is a fuzzy measure of the relative
  # heft of page in memory.
  #
  # An exact mesure is not necessary. We only need to be sure to trigger a cache
  # purge at some point before we reach the limits imposed by sytem memory or
  # the V8 JavaScript engine.
  #
  # #### Cache Entries
  #
  # Cache entries are the page objects themselves.
  #
  # It is important that the page objects are unique, that we do not represent a
  # page file with more than one page object, because the page objects house the
  # locking mechanisms. The page object acts as a mutex for page data.
  #
  # The cache entires are linked to form a doubly-linked list. The doubly-linked
  # list of cache entries has a head node that has a null address, so that end
  # of list traversal is unambiguous.
  #
  # We always move a page to the front of the core list when we reference it
  # during b&#x2011;tree descent.

  # Create an most-recently used list head node and return it. We call this to
  # create the core and balance list in the constructor above.
  createMRU: ->

  # Link tier to the head of the most-recently used list.
  link: (head, entry) ->
    next = head.next
    entry.next = next
    next.previous = entry
    head.next = entry
    entry.previous = head
    entry

  # Unlnk a tier from the most-recently used list.
  unlink: (entry) ->
    { next, previous } = entry
    next.previous = previous
    previous.next = next
    entry

  # ### Leaf Pages
  #
  # Five key things to know about leaf pages.
  #
  # * A leaf page is an array of records.
  # * The key of the first record is the key for the page.
  # * If the first record is deleted, we keep a it as a ghost record, for the
  # sake of the key, until the leaf page can be vacuumed.
  # * The leaf page file is a text file of JSON strings that is an append log of
  # record insertions and deletions.
  # * A leaf page cannot contain two records that share the same key, therefore
  # the b&#x2011;tree cannot contain duplicates.
  #
  # #### Constant Time
  #
  # In the abstract, a leaf page is an array of records.  Given an integer, the
  # leaf page will return the record stored at the offset of the array. This
  # lookup is performed in constant time when the record is in memory.
  #
  # This lookup is performed in more or less constant time when the record is
  # uncached, if you're willing to say that random access into a file is
  # constant time for practical purposes, otherwise it is *O(log n)*, where *n*
  # is the number of blocks in the leaf page.
  #
  # #### Binary Search
  #
  # Our leaf page implemenation maintains an array of file positions called a
  # positions array. A file position in the positions array references a record
  # in the leaf page file by its file position. The positions in the positions
  # array are sorted according to the b&#x2011;tree collation of the referenced
  # records.
  #
  # In the leaf page file, a record is stored as JSON string. The objects are
  # loaded from the file as needed, or else when the opportunity presents
  # itself. The leaf page keeps a map (a JavaScript `Object`) that maps file
  # positions to deserialized records.
  #
  # Because the records are sorted, and because a lookup takes constant time, we
  # can search for a record in a leaf page using binary search in logorithmic
  # time.
  #
  # #### No Duplicates
  #
  # Leaf pages cannot contain duplicate records. Therefore, the b&#x2011;tree cannot
  # contain duplicate records. You can simulate duplicate records by adding a
  # series value to your key. The cursor implementation is designed faciliate
  # psuedo-duplicates in this fashion.
  #
  # #### Leaf Page Key
  #
  # The first record of every leaf page is the key value of the leaf page.
  #
  # When we delete records from the leaf page, if we delete the first reord, we
  # keep a ghost of the record around, so we will know the key value of the leaf
  # page.
  #
  # #### Leaf Page Split
  #
  # If the record count of leaf page exceeds the leaf order, the leaf page is split.

  # The in memory representation of the leaf page includes a flag to indicate
  # that the page is leaf page, the address of the leaf page, the page address
  # of the next leaf page, and a cache that maps record file positions to
  # records that have been loaded from the file.
  createLeaf: (address, override) ->
    page = @cache[address] = @link @mru,
      balancers: 0
      loaded: false
      leaf: true
      address: address
      positions: []
      cache: {}
      deleted: 0
      count: 0
      size: 0
      right: -1
      locks: [[]]
    extend page, override or {}

  # #### Leaf Page JSON Size
  #
  # JSON size is used as a fuzzy measure of the in&#x2011;memory size of a leaf page.

  # #### Cached JSON Page Size

  # We have to cache the calcuated size of the record because we return the
  # records to the client.  We need to cache the JSON size and the key value
  # when we load the object so we can deduct the proper amount from the page
  # size and total size when we delete a record. We can't recalculate because
  # we're not strict about ownership. The application programmer may decide to
  # alter the object we returned.
  cacheRecord: (page, position, record) ->
    key = @extractor record

    size = 0
    size += JSON.stringify(record).length
    size += JSON.stringify(key).length

    entry = page.cache[position] = { record, key, size }

    page.size += size
    @size += size

    entry

  # We do not include the position size in the cached size because it is simple
  # to calculate and the client cannot alter it.
  cachePosition: (page, position) ->
    size = if page.length is 1 then "[#{position}}" else ",#{position}"

    page.size += size
    @size += size

  # When we purge the record, we add the position length. We will only ever
  # delete a record that has been cached, so we do not have to create a function
  # to purge a position.
  purgeRecord: (page, position) ->
    if size = page.cache[position]?.size
      size += if page.length is 1 then "[#{position}}" else ",#{position}"

      page.size -= size
      @size -= size

      delete page.cache[position]

  # ### Appends for Durability
  #
  # A leaf page file contains JSON objects, one object on each line. The objects
  # represent record insertions and deletions, so that the leaf page file is
  # essentially a log. Each time we write to the log, we open and close the
  # file, so that the operating system will flush our writes to disk. This gives
  # us durability.
  # 
  # Append an object to the leaf page file as a single line of JSON.
  #
  # We call the append method to both append new records to an existing leaf
  # page file, as well as to create whole new replacment leaf page file that
  # will be relinked to replace the existing leaf page file. The caller
  # determines which file should be written, so it opens and closes the file
  # descriptor.
  #
  # The file descriptor must be open for for append.

  #
  _writeJSON: (fd, page, object, _) ->
    page.position or= fs.fstat(fd, _).size

    # Calcuate a buffer length. Take note of the current page position.
    json            = JSON.stringify object
    position        = page.position
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
      written = fs.write fd, buffer, offset, count, page.position, _
      page.position += written
      offset += written

    # Return the file position of the appended JSON object.
    position

  # ### Leaf Page File Records
  #
  # TODO address array objects are actually reference objects, or position
  # objects? Right.
  #
  # There are three types of objects in a leaf tier file, ***insert objects***,
  # ***delete objects***, and ***position array objects***.
  #
  # An insert object contains a ***record*** and the index in the position array
  # where the record's position would be inserted to preserve the sort order of
  # the position array.
  #
  # Beginning with an empty position array and reading from the start of the
  # file, the leaf tier is reconstituted by replaying the inserts and deletes
  # described by the insert and delete objects.
  #
  # The JSON objects stored in the leaf array are JSON arrays. The first element
  # is used as a flag to indicate the type of object.
  #
  # If the first element is an integer greater than zero, it indicates an insert
  # object.  The integer is the one based index into the zero based position
  # array, indicating the index where the position of the current insert object
  # should be inserted. The second element of the leaf tier object array is the
  # record object.
  #
  # When we read the insert object, we will place the record in the record cache
  # for the page, mapping the position to the record.

  # Write an insert object. Calculate the serialized JSON string length of the
  # inserted record and add it to the in memory JSON size of the page and the in
  # memory b&#x2011;tree as a whole. We always use the JSON serialization we already
  # perform for storage, instead of serializing for both storage and size
  # calculation.
  writeInsert: (fd, page, index, record, _) ->
    @_writeJSON fd, page, [ index + 1, record ], _

  # If the first element is less than zero, it indicates a delete object. The
  # absolute value of the integer is the one based index into the zero based
  # position array, indicating the index of address array element that should be
  # deleted.
  #
  # There are no other elements in the delete object.

  # Write a delete object. Calculate the serialized JSON string length of the
  # inserted record and add it to the in memory JSON size of the page and the in
  # memory b&#x2011;tree as a whole.
  #
  # TODO Document `ghost`.
  writeDelete: (fd, page, index, ghost, _) ->
    @_writeJSON fd, page, [ -(index + 1), ghost ], _
  
  # On occasion, we can store a position array object. An position array object
  # contains the position array itself.  We store a copy of a constructed
  # position array object in the leaf page file so that we can read a large leaf
  # page file quickly.
  #
  # When we read a leaf page file, if we read from the back of the file toward
  # the front, we can read backward until we find an position array object. Then
  # we can read forward to the end of the file, applying the inserts and deletes
  # that occured after we wrote the position array object. 
  # 
  # When a leaf page file is large, stashing the constructed position array at
  # the end means that the leaf page can be loaded quickly, because we will only
  # have to read backwards a few entries to find a mostly completed position
  # array. We can then read forward from the array to amend the position array
  # with the inserts and deletes that occured after it was written.
  #
  # Not all of the records will be loaded when we go backwards, but we have
  # their file position from the address array, so we can jump to them and load
  # them as we need them. That is, if we need them, because owing to binary
  # search, we might only need a few records out of a great many records to find
  # the record we're looking for.

  # Write an address array object.
  writePositions: (fd, page, _) ->
    @_writeJSON fd, page, [ 0, page.right, page.positions ], _

  # Here is the backward search for a position in array in practice. We don't
  # really ever start from the beginning. The backwards than forwards read is
  # just as resillient.
  
  #
  readLeaf: (page, _) ->
    # We don't cache file descriptors after the leaf page file read. We will
    # close the file descriptors before the function returns.
    filename  = @filename page.address
    fd        = fs.open filename, "r+", _
    stat      = fs.stat filename, _

    # When we read backwards, we create a list of of the insert and delete
    # objects in the splices array. When we have found a positions array, we
    # stop going backwards.
    splices   = []
    # Note that if we don't find a position array that has been written to the
    # leaf page file, then we'll start with an empty position array.
    positions = []
    #
    line      = ""
    offset    = -1
    end       = stat.size
    eol       = stat.size
    buffer    = new Buffer(1024)
    # TODO You can edit your files, and you can certainly read them, but know
    # that they are fragile. We treat extra whitespace as corruption, an
    # indication that something is wrong. We're not forgiving, because that
    # would complicate the code, also introduce ambigutities. If this were a
    # binary file format, there would be no forgiveness. If we were truly a
    # human format, then certianly there would be forgiveness, but we're not,
    # not really a text format for editing, only one for sanity checking. Thus a
    # line alway ends with `"]\n"`, so we know that something is wrong. If the
    # last line does not end this way, it is treated as a bad write and the
    # record is discarded. Now, we could do that, but the chances that a
    # developer will dip into the files and make an edit are rather high. Hmm...
    # But we store file positions, so making an edit will corrupt the files.
    #
    # TODO Thinking about using SHA1 as a checksum and resuming it for each line.
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
          object = JSON.parse buffer.toString("utf8", read, eos)
          eos   = read + 1
          index = object.shift()
          if index is 0
            page.right = object.shift()
            page.positions = object.shift()
            end = 0
            break
          else
            position = start + read + 1
            splices.push [ index, position ]
            if index > 0
              @cachePosition(page, position)
              @cacheRecord(page, position, object.shift())
      eol = start + eos
    # Now we replay the inserts and deletes described by the insert and delete
    # objects that we've gathered up in our splices array.
    splices.reverse()
    for splice in splices
      [ index, position ] = splice
      if index > 0
        positions.splice(index - 1, 0, position)
      else
        positions.splice(-(index + 1), 1)
    # Close the file descriptor.
    fs.close fd, _
    
    # Return the loaded page.
    count = positions.length
    extend page, { positions, count, loaded: true }

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

  # Our backwards read can load a position array that has been written to the
  # the leaf page file, without having to load all of the records referenced by
  # the position array. We will have to load the records as they are requested.
  #
  # To load a record, we open the file and jump to the position indicated by the
  # position array. We then read the insert object that introdced the record to
  # the leaf page file.
  #
  # We open a file descriptor and then close it after the record has been read.
  # The desire to cache the file descriptor is strong, but it would complicate
  # the shutdown of the b&#x2011;tree. As it stands, we can always simply let the
  # b&#x2011;tree succumb to the garbage collector, because we hold no other system
  # resources that need to be explictly released.
  #
  # Note how we allow we keep track of the minimum buffer size that will
  # accommodate the largest possible buffer.
  #
  # TODO Have a mininum buffer that we constantly reuse, uh no. That will be
  # shared by descents.

  #
  readRecord: (page, position, _) ->
    filename = @filename page.address
    page.position or= fs.stat(filename, _).size
    fd = fs.open filename, "r", _
    loop
      buffer = new Buffer(@length)
      read = fs.read fd, buffer, 0, buffer.length, position, _
      if json = @_readJSON(buffer, read)
        break
      if @length > page.position - position
        throw new Error "cannot find end of record."
      @length += @length >>> 1
    fs.close fd, _
    json.pop()
  

  # Over time, a leaf page file can grow fat with deleted records. Each deleted
  # record means both an insert object that is no longer useful, and the delete
  # record that marks it as useless.  We vacuum a leaf page file by writing it
  # to a replacement leaf page file, then using `relink` to replace the current
  # leaf page file with the replacement.
  #
  # All of records referenced by the current position array are appended into
  # the replacement leaf page file using insert objects. A position array object
  # is appended to the end of the replacement leaf page file. The rewritten leaf
  # page file will load quickly, because the position array object will be found
  # immediately.

  # Note that we close the file descriptor before this function returns.
  rewriteLeaves: (page, _) ->
    filename = @filename page.address, ".new"
    fd = fs.open filename, "a", 0644, _
    positions = []
    cache = {}
    for position, index in page.positions
      object = page.cache[position] or @readRecord page, position, _
      position = @writeInsert fd, page, index, object, _
      positions.push position
      cache[position] = object
    extend page, { positions, cache }
    @writePositions fd, page, _
    fs.close fd, _

  # ### Branch Pages
  #
  # Five key things to know about branch pages.
  #
  # * A branch page contains an array of addresses of child pages.
  # * The left most address is the left child of the entire branch page.
  # * The branch page keys are looked up as needed  by descending the tree to
  # the left most leaf page of a child and using the leaf page page key.
  # * The root branch page is always at address `0`.
  #
  # To find the a record in the b&#x2011;tree, we first use a tree of branch pages to
  # find the leaf page that contains our record.
  #
  # A branch page contains the addresses of child pages. This array of page
  # addresses is essentially an *array of children*.
  #
  # The child addresses are ordered according to the b&#x2011;tree collation of the
  # keys of the directly or indirectly referenced leaf pages.
  #
  # There are three types of branch pages, penultimate branch pages, interior
  # branch pages, and the root branch page.
  #
  # #### Penultimate Branch Pages
  #
  # A penultimate branch page is a branch page whose children are leaf pages. If
  # a branch page is not penultimate, then its children are branch pages.
  #
  # In a penultimate branch page, the array of children is ordered by the
  # b&#x2011;tree
  # collation using a first record in the referenced leaf page for ordering.
  # That is, the first record of the leaf page is used as the key associated
  # with a page address in a penultimate branch page.
  #
  # The non-leaf nodes of a b&#x2011;tree have the property that the number of node
  # children is one greater than the number of keys. We obtain this property by
  # treating the first child as the left child of the entire page, and excluding
  # its key from the search. We search the subsequent keys to find the first key
  # that is grater than or equal to the record sought. Essentially, when we
  # encouter a key that is greater than our sought record, we know that the
  # record is contained in the leaf page child associated with the key before
  # it. We are able to perform this search using binary search in logorithmic
  # time.
  #
  # By ignoring the key of the first leaf page, the penultimate branch page has
  # a number of children that is one greater than the number of keys.
  #
  # Notice that, when we are inserting a record into a leaf page other than the
  # left leaf page, we add it to a leaf page whose key is equal to or greater
  # than the penultimate branch key, so that the first record does not change,
  # and therefore that penultimate branch key does not change. The exception is
  # the left leaf page, which accepts all the records less than the first key,
  # and therefore may accept a record less than its current least record.
  #
  # An insertion can only insert a into the left most leaf page of a
  # penumltimate branch page a record less than the least record of the leaf
  # page.
  #
  # #### Interior Branch Pages
  #
  # A branch page whose children are other branch pages is called an interior
  # branch page.
  #
  # Like the penultimate branch page, we treat the first child of an interior
  # branch page as the left child of the entire page. Like the penultimate
  # branch page the subsequent children have an associated key that is the first
  # record of a leaf page.
  #
  # The key is obtained by decending the sub&#x2011;tree referenced by the child. We
  # first visit the branch page referneced by the child. We then visit left
  # children recursively, visiting the left child of the child, and the left
  # child of any subsquently visited children, until we reach a leaf page.  The
  # first record of that leaf page is the key to associate with the child
  # address in the address array of the interior branch page.
  #
  # It is the nature of the b&#x2011;tree that keys move up to the higher levels of the
  # tree as pages split, while preserving the collation order of the keys. When
  # a branch page splits, a key from the middle of the page is chosen as a
  # partition. The partition is used as the key for the right half of the split
  # page in the parent page.
  #
  # Our implementation does not store the keys, as you may have noticed, but
  # decends down to the leaf page to fetch the record to use as a key. 
  #
  # We start from a penultimate page as a root page. When a leaf page fills, we
  # split it, creating a new right leaf page. The penultimate page uses the
  # first record of the new right page as the key to associate with that page.
  #
  # When the root penultimate page is full we split it, so that the root page is
  # an interior page with two children, which are two penultimate pages. The
  # tree now contains a root interior branch page, with a left penultimate
  # branch page and a right penultimate branch page.
  #
  # The root interior branch page has one key. Prior to split, that key was
  # associated with the address of a child leaf page. After split, the key is
  # associated with the right penultimate branch page. The leaf page is now the
  # left child of the right penultimate branch page.
  #
  # When we visit the root interior page, to obtain the key to associate with
  # the right penultimate page, we visit the right penultimate page, then we
  # visit its left child, the leaf page whose first record is the key.
  #
  # #### Root Branch Page
  #
  # The root page is the first page we consult to find the desired leaf page.
  # Our b&#x2011;tree always contains a root page. The b&#x2011;tree is never so empty that
  # the root page disappears. The root page always has the same address.
  #
  # TK move. Until the root branch page is split, it is both the root branch
  # page and a penultimate branch page.
  #
  # ### Keys and First Records
  #
  # We said that it is only possible for an insertion to insert a into the left
  # most child leaf page of a penumltimate branch page a record less than the
  # least record. We can say about a tree rooted by an interor branch page, that
  # an insertion is only able to insert into the left most leaf page in the
  # *entire tree* a record less than the least record.
  #
  # Using our example tree with one root interior page, with two penultimate
  # branch page children, we cannot insert a record into the right penultimate
  # branch page that will displace the first record of its left most child
  # branch, because that first record is the key for the right penultimate
  # branch page.  When we insert a record that is less than the key, the search
  # for a leaf page to store the record goes to the left of the key. It cannot
  # descend into the right penultimate branch page, so it is impossible for it
  # be inserted into left child of the right penultimate branch page, so the
  # first record left child of the penultimate branch page will never be
  # displaced by an insertion.
  #
  # Only if we insert a record that is less than least key of the left
  # penultimate page do we face the possibility of displacing the first record
  # of a leaf page, and that leaf page is the left most leaf page in the entire
  # tree.
  #
  # This maintains a property of the b&#x2011;tree that for every leaf page except the
  # left most leaf page, there exists a unique branch page key derived from the
  # first record of the page.
  #
  # As above, you can find the first record used to derive a key by visting the
  # child and going left. You can find the leaf page to the left of the leaf
  # page used to derive a page branch key, by visiting the child to the left of
  # the key and going right.
  #
  # NOTE Literate programming has finally materialized with Docco and
  # CoffeeScript.
  #
  # When the root page splits, it becomes an interior branch page. Until it
  # splits it is both the root page and a penultimate page.

  # ### Branch Page Files
  #
  # We create a new branch pages in memory. They do not exist on disk until they
  # are first written.
  #
  # A new branch page is given the next unused page number.
  #
  # In memory, a branch page is an array of child page addresses. It keeps track
  # of its key and whether or not it is a penultimate branch page. The cache is
  # used to cache the keys associated with the child page addresses. The cache
  # maps the address of a child page to a key extracted from the first record of
  # the leaf page referenced by the child page address.
  #
  # Our in memory is also cached and added as a node an MRU list. We must make
  # sure that each page has only one in memory representation, because the in
  # memory page is used for locking.

  #
  createBranch: (address, override) ->
    page = @cache[address] = @link @mru,
      balancers: 0
      count: 0
      penultimate: true
      address: address
      addresses: []
      cache: {}
      locks: [[]]
      loaded: false
      right: -1
      size: 0
    extend page, override or {}

  # We write the branch page to a file as a single JSON object on a single line.
  # We tuck the page properties into an object, and then serialize that object.
  # We do not store the branch page keys. They are looked up as needed as
  # described in the b&#x2011;tree overview above.
  #
  # We always write a page branch first to a replacement file, then move it
  # until place using `relink`.

  #
  rewriteBranches: (page, _) ->
    filename = @filename page.address, ".new"
    record = [ page.right, page.addresses ]
    json = JSON.stringify(record)
    buffer = new Buffer(json.length + 1)
    buffer.write json
    buffer[json.length] = 0x0A
    fs.writeFile filename, buffer, "utf8", _

    # Update in memory serialized JSON size of page and b&#x2011;tree.
    @size -= page.size or 0
    page.cache = {}
    page.size = JSON.stringify(page.addresses).length
    @size += page.size

  # To read a branch page we read the entire page and evaluate it as JSON. We
  # did not store the branch page keys. They are looked up as needed as
  # described in the b&#x2011;tree overview above.

  #
  readBranches: (page, _) ->
    filename = @filename page.address
    json = fs.readFile filename, "utf8", _
    record = JSON.parse json
    [ right, addresses ] = record
    count = addresses.length

    # Set in memory serialized JSON size of page and add to b&#x2011;tree.
    page.size = JSON.stringify(addresses).length
    @size += page.size

    # Extend the existing page with the properties read from file.
    extend page, { right, addresses, count }

  # Add a key to the branch page cache and recalculate JSON size.
  cacheKey: (page, address, key) ->
    size = JSON.stringify key

    page.cache[address] or= {}
    page.cache[address].key = key
    page.cache[address].size = size

    page.size += size
    @size += size

  # Purge a key from the branch page cache and recalculate JSON size.
  purgeKey: (page, address) ->
    if size = page.cache[address]?.size
      page.size -= size
      @size -= size
      delete page.cache[address]

  # ### B-Tree Initialization
  #
  # After creating a `Strata` object, the client will either open the existing
  # database, or create a new database.
  #
  # #### Creation
  #
  # Creating a new database will not create the database directory. The database
  # directory must already exist, it must be empty. We don't want to surprise
  # the application developer by blithely obliterating an existing database.
  #
  # An empty database has a single root penultimate branch page with only a left
  # child and no keys. The left child is a single leaf page that is empty.
  #
  # Note that the address of the root branch page is `0` and the address of the
  # left most leaf page is `-1`. This will not change. Even as the b&#x2011;tree is
  # balanced with splits and mergers of leaf pages, the root branch page is
  # always `0` and left most leaf page is always `-1`.

  #
  create: (_) ->
    # Create the directory if it doesn't exist.
    stat = fs.stat @directory, _
    if not stat.isDirectory()
      throw new Error "database #{@directory} is not a directory."
    if fs.readdir(@directory, _).filter((f) -> not /^\./.test(f)).length
      throw new Error "database #{@directory} is not empty."
    # Create a root branch with a single empty leaf.
    root = @createBranch @nextAddress++, penultimate: true, loaded: true
    leaf = @createLeaf -(@nextAddress++), loaded: true
    root.addresses.push leaf.address
    # Write the root branch.
    @rewriteBranches root, _
    @rewriteLeaves leaf, _
    @relink leaf, _
    @relink root, _

  # #### Opening
  #
  # Opening an existing database is a matter checking for any evidence of a hard
  # shutdown. You never know. There may be a banged up leaf page file, one who's
  # last append did not complete. We won't know that until we open it.
  #
  # Ah, no. Let's revisit. Here's a simple strategy. Open touches a file.
  # Closing deletes teh file. If we open and the file exists, then we probably
  # have to inspect every file that was modified after the modification,
  # adjusting for dst? No because we'll be using seconds since the epoch. Only
  # if the system time is changed do we have a problem.
  #
  # Thus, we have a reference point. Any file after needs to be inspected. We
  # load it, and our `readLeaf` function will check for bad JSON, finding it
  # very quickly.
  #
  # Now, we might have been in the middle of a split. The presenence of `*.new`
  # files would indicate that. We can probably delete the split. Hmm..
  #
  # We can add more to the suffix. `*.new.0`, or `*.commit`, which is the last
  # relink. If we have a case where there is a file named `*.commit` that does
  # not have a corresponding permanent file, then we have a case where the
  # permenant file has been deleted and not linked, but all the others have
  # been, since this operaiton will go last, so we complete it to go forward.
  #
  # Otherwise, we delete the `*.commit`. We delete all the replacments that are
  # not commits.
  #
  # We can always do a thorough rebuild of some kind.
  #
  # Probably need "r" to not create the crash file, in case we're reading from a
  # read only file system, or something..
  
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

  close: (_) ->

  # TODO Ensure that you close the current tier I/O. Also, you must also be very
  # much locked before you use this, but I'm sure you know that.

  # ### Concurrency
  #
  # The b&#x2011;tree must only be read and written by a single Node.js process. It is
  # not suitable for use with multiple node processes, or the cluster API.
  #
  # Although there is only a single thread in a Node.js process, the
  # b&#x2011;tree is
  # still a concurrent data structure. Instead of thinking about concurrency in
  # terms of threads we talk about concurrent *descents* of the b&#x2011;tree.
  #
  # When we search the tree or alter the tree, we must descend the tree.
  #
  # Decents of the b&#x2011;tree can become concurrent when descent encounters a page
  # that is not in memory. While it is waiting on evented I/O to load the page
  # files, the main thread of the process can make progress on another request
  # to search or alter the b&#x2011;tree, it can make process on another descent.
  #
  # This concurrency keeps the CPU and I/O loaded.
  #
  # ### Locking
  #
  # Locking prevents race conditions where an evented I/O request returns to to
  # find that the sub&#x2011;tree it was descending has been altered in way that causes
  # an error. Pages may have split or merged by the main thread, records may
  # have been inserted or deleted. While evented I/O is performed, the
  # sub&#x2011;tree
  # needs to be locked to prevent it from being altered.
  #
  # The b&#x2011;tree is locked page by page. We are able to lock only the pages of
  # interest to a particular descent of the tree.
  #
  # Futhermore, the b&#x2011;tree destinguishes between shared read locks and exclusive
  # write locks. Multiple descents can read a traverse that is read locked, but
  # only the descent that holds an exclusive write lock can traverse it or write
  # to it.
  #
  # Of course, Node.js doesn't have the concept of mutexes to protect critical
  # sections of code the way that threaded programming platforms do. There are
  # no standard read and write lock APIs for use to use.
  #
  # Nor do we use file system locking.
  #
  # Instead, we simulate locks using callbacks. A call to `lock` is an evented
  # function call that provides a callback. If the `lock` method can grant the
  # lock request to the caller, the lock method will invoke the callback.
  #
  # If the `lock` method cannot grant the lock request, the `lock` method will
  # queue the callback into a queue of callbacks assocated with the page. When
  # other descents release the locks that prevent the lock request, the lock
  # request callback is dequeued, and the callback invoked.
  #
  # The locking mechanism is a writer preferred shared read, exclusive write
  # lock. If a descent holds an exclusive write lock, then all lock requests by
  # other descents are blocked. If one or more descents hold a shared read lock,
  # then any request for an exclusive write lock is blocked. Any request for a
  # shared read lock is granted, unless an exclusive write lock is queued. 
  #
  # The locks are not re-entrant.
  #
  # #### Lock

  # TODO Signature: Address should go first.
  #
  # TODO If the page is not loaded, and we simply move forward when we encounter
  # the page, that is, we have two descents, one encounters this unloaded page,
  # but it is a read, so we go ahead, it makes an evented I/O call and waits,
  # we make progress on a new descent, the desent encounters the unloaded page,
  # and it loads the page, and waits.
  #
  # Maybe we can queue the people waiting on the load? The simplest thing would
  # be to put the callbacks in an array of onloads.

  #
  lock: (address, exclusive, callback) ->
    # We must make sure that we have one and only one page object to represent
    # the page. We the page object will maintain the lock queue for the page. It
    # won't due to have different descents consulting different lock queues.
    # There can be only one.
    #
    # The queue is implemented using an array of arrays. Shared locks are
    # grouped inside one of the arrays in the queue element. Exclusive locks are
    # queued alone as a single element in the array in the queue element.
    if not page = @cache[address]
      page = @["create#{if address < 0 then "Leaf" else "Branch"}"](address)

    # If the page needs to be laoded, we must load the page only after a lock
    # has been obtained. Loading is a read, so we can load regardless of whether
    # the lock is exclusive read/write or shared read.
    if page.loaded
      lock = callback
    else
      lock = (error, page) =>
        if error
          callback error
        else if page.loaded
          callback null, page
        else if address < 0
          @readLeaf page, callback
        else
          @readBranches page, callback

    # The callback is always added to the queue, even if it is not blocked and
    # will execute immediately. The array in the queue element acts as a lock
    # count.
    #
    # If the callback we add to the queue is added to the the first queue
    # element is executed immediately. Otherwise, it will be executed when the
    # preceeding queue elements have compeleted.
    #
    # When an exclusive lock is queued, an empty array is appended to the queue.
    # Subsequent read lock callbacks are appened to the array in the last
    # element. This gives exclusive lock callbacks priority.
    locks = page.locks
    if exclusive
      throw new Error "already locked" unless locks.length % 2
      locks.push [ lock ]
      locks.push []
      if locks[0].length is 0
        locks.shift()
        lock(null, page)
    else
      locks[locks.length - 1].push lock
      if locks.length is 1
        lock(null, page)

  # #### Unlock

  # When we release a lock, we simply shift a callback off of the array in the
  # first element of the queue to decrement the lock count. We are only
  # interested in the count, so it doesn't matter if the callback shifted by the
  # descent is the one that it queued.

  #
  unlock: (page) ->
    locks = page.locks
    locked = locks[0]
    locked.shift()
    if locked.length is 0 and locks.length isnt 1
      locks.shift()
      # Each callback is scheduled using next tick. If any callback waits on
      # I/O, then another one will resume. Concurrency.
      for callback in locks[0]
        do (callback) -> process.nextTick -> callback(null, page)
      
  upgrade: (tier, _) ->
    @unlock tier
    @lock tier.address, true, false, _

  stash: (page, index, _) ->
    position = page.positions[index]
    if not stash = page.cache[position]
      record = @readRecord page, position, _
      stash = @cacheRecord page, position, record
    stash

  # TODO Descend. Ah, well, find is in `Descent`, so this moves to `Descent`.
  key: (page, index, _) ->
    stack = []
    loop
      if page.leaf
        key = @stash(page, index, _).key
        break
      else
        address = page.addresses[index]
        page = @lock address, false, page.penultimate, _
        index = 0
        stack.push page
    for page in stack
      @unlock page
    key

  # Binary search implemented, as always, by having a peek at [Algorithms in
  # C](http://www.informit.com/store/product.aspx?isbn=0201314525) by [Robert
  # Sedgewick](http://www.cs.princeton.edu/~rs/).
  find: (page, key, low, _) ->
    { comparator } = @
    high = page.count - 1
    # Classic binary search.
    while low <= high
      mid = (low + high) >>> 1
      compare = comparator key, @key(page, mid, _)
      if compare > 0
        low = mid + 1
      else if compare < 0
        high = mid - 1
      else
        return mid
    # Index is negative if not found.
    ~low

# ## Descent
#
# We use the term *descent* to describe b&#x2011;tree operations, because all
# b&#x2011;tree
# operations require a descent of the b&#x2011;tree, a traversal of the b&#x2011;tree starting
# from the root. Whenever we are search the tree, insert or delete records, or
# balance the tree with a page splits and merges, we first begin with a descent
# of the b&#x2011;tree, from the root, to find the page we want to act upon.
#
# #### Descent as Unit of Work
#
# We use the term descent to describe the both traversal of the b&#x2011;tree and the
# subsequent actions performed when when the desired page is found.
# 
# The descent is the unit of work in our concurrency model.  A descent is
# analogous to a thread, because when a descent waits on I/O, other descents can
# make progress.
#
# Descents can make progress concurrently, even though Node.js only has a single
# thread of execution. Descents do not actually make progress in parallel, but
# their progress can be interleaved. When we descend the tree, we may have to
# wait for evented I/O to read or write a page. While we wait, we can make
# progress on another descent in the main thread of execution.
#
# Because descents can make interleaved progress, we need to synchronize access
# to b&#x2011;tree pages, just as we would with a multi-threaded b&#x2011;tree implementation.
# When we descend the b&#x2011;tree we need to make sure that we do not alter pages
# that another waiting descent needs to complete its descent when it awakes, nor
# read pages that a waiting descent had begun to alter before it had to wait.
#
# These are race conditions. We use the shared read/exclusive write locks
# described in the `IO` class above to guard against these race conditions.
#
# #### Classes of Descent
#
# When we descend to leaf pages of a search b&#x2011;tree to obtain records, we
# *search* the b&#x2011;tree. When we change the size of the b&#x2011;tree by adding or
# deleting records we *edit* the b&#x2011;tree. When we change the structure of the
# b&#x2011;tree by splitting or merging pages, we *balance* the b&#x2011;tree.
#
# We talk about search descents, edit descents, and balance descents we we
# describe the interaction of b&#x2011;tree operations.
#
# We use these terms in this document to save the chore of writing, and the
# confustion of reading; insert or delete, or split or merge. We also want to
# draw a distinction between changing the count of records stored in the
# b&#x2011;tree,
# *editing*, and changing the height of the b&#x2011;tree, the count of pages, or the
# choice of keys, *balancing*.
#
# #### Locking on Descent
#
# Becase a search descent does not alter the structure of the b&#x2011;tree, Multiple
# search descents can be performed concurrently, without interfering with each
# other.
#
# Descents that alter the b&#x2011;tree exclusive access, but only to the pages they
# alter. A search descent can still make progres in the presence of an
# alteration decent, so long as the search does not visit the pages being
# altered.
#
# A search descent obtains shared locks on the pages that it visits.  An
# alteration descent obtains exclusive locks on the pages that it needs to
# alter. The alteration descent will obtain shared locks on the pages that
# visits in search the pages that it wants to alter.
#
# #### Locking Hand Over Hand
#
# To allow progress in parallel, we lock only the pages we need to descend the
# tree, for only as long as we takes to determine which page to visit next.
# Metaphically, we descend the tree locking hand-over-hand.
#
# We start from the root page. We lock the root page. We perform a binary search
# that compares our search key against the keys in the root page. We determine
# the correct child page to visit to continue our search. We lock the child
# page. We then release the lock on the parent page.
# 
# We repeat the process of locking a page, searching it, locking a child, and
# then releasing the lock on the child's parent.
#
# We hold the lock on the parent page while we aquire the lock on the child page
# because we don't want another descent to alter the parent page, invaliding the
# direction of our descent.
#
# #### Lateral Traversal
#
# Both branch pages and leaf pages are singly linked to their right sibling. If
# you hold a lock on a page, you are allowed to obtain a lock on its right
# sibling. This left right ordering allows us to traverse a level of the
# b&#x2011;tree,
# which simplifies the implemtation of record cursors and page merges.
#
# When we move from a page to its right sibling, we hold the lock on the left
# page until we've obtained the lock on the right sibling. The prevents another
# descent from relinking linking our page and invalidating our traversal.
#
# #### Deadlock Prevention and Traversal Direction
#
# To prevent deadlock, we always move form a parent node to a child node, or
# form a left sibling to a right sibling.
# so that we do not create a condition
# where on descent 
#
# Othewise we would deadlock when a descent that has an exclusive lock on a
# parent attempted to obtain a lock on child, while another descent has either
# sort of lock on the child attempted to obtain a lock on the parent. By only
# obtaining locks top down, we avoid this deadlock condition because all of the
# descents obtain locks in the same order.
#
# When traversing the tree laterally, we always travel from a page to the right
# sibling of that page. Two descents traversing a level in both directions would
# deadlock when they encountered each other, if one of the descents was locking
# exclusively.
#
# To prevent a deadlock when a left right traversal coincides with the top down
# traversal, we insist that when a parent obtains a lock on more than one child,
# it locks the children in left to right order. That is, we must remember the
# left right ordering regardless of whether we're navigating using a page's link
# to it's right sibling, or whether we're referencing a branch page's children
# pages.

class Descent
  # The constructor always felt like a dangerous place to be doing anything
  # meaningful in C++ or Java.
  constructor: (@io) ->
    @locks      = []
    @locked     = {}
    @first      = true
    @page       = { addresses: [ 0 ], leaf: false }
    @index      = 0
    @exact      = false
    @exclusive  = false

  key: (key) ->
    { io } = @
    (page, _) -> io.find page, key, 1, _

  iterate: (page) ->
    @first = false

  leftMost: (page, _) -> 0

  penultimate: (page) -> page.addresses[0] < 0

  leaf: (page) -> page.leaf

  exclude: -> @exclusive = true

  descend: (next, stop, _) ->
    while not stop(@page, @index, @exact)
      parent = @page
      @page = @io.lock parent.addresses[@index], @exclusive, _
      @io.unlock parent if parent.address?
      @index = next @page, _
      if @index < 0
        @index = (~@index) - 1
        @exact = false
      else
        @exact = true
      @first and= @index is 0
          
# ## Cursors
# 
# When the application developer requests a cursor, they receive either a read
# only *iterator*, or a read/write *mutator*.
#
# An interator provides random access to the records in a page. It can move from
# a page to the right sibling of the page. A mutator does the same, but it is
# also able to insert or delete records into the current page.
#
# ### Iterator
#
# The application developer uses an iterator to move across the leaf pages of
# the b&#x2011;tree in ascending collation order, one leaf page a time. The leaf pages
# themselves are visited one at a time, not the records. The iterator can
# randomly access any record a the currently visited leaf page.
#
# The application developer obtains an iterator by calling `Strata.cursor` and
# providing a key. The key is used to find the page that would contain the key.
# Alternatively, the application developer can obtain an iterator that begins at
# the left most leaf page by by calling `Strata.first`. 
#
# By locking the pages left to right hand over hand, then there is now way for
# the tree to mutate such that would defeat our iteration. Leaf pages that we've
# visited may by edited by another descent after we've visited them, however.

#
class Iterator
  # #### Ambiguous Range Start
  #
  # As the result of a descent, we will know the index of the record that
  # corresponds to the key used to find the leaf page. This is either the index
  # of the key's record, or if there no record that could derive the key in the
  # b&#x2011;tree, the location where the key's record would be inserted.
  #
  # There are good reasons to search for key that does not exist in the tree. We
  # may be interested in searching for time series data that occured between
  # noon and midnight. It doesn't matter to us if there were no events that
  # occured exactly at the millisecond that defines noon.
  #
  # We are interested in starting our iteration from where that record would be.
  # That is the insert location of the millisecond that defines noon. We need a
  # way to find an ***insert location*** for a record, in order to implement a
  # ranged search.
  #
  # When we ask for the insert location of a key, if the key is greater than the
  # largest record in the leaf page, then the index is the end of the leaf page.
  # If this is the case, the real insert location might be in the right sibling
  # leaf page, or in a page beyond the right sibling leaf page. We won't know
  # for certain if the non-existing record would be in the current leaf page
  # without testing to see if it is less than the first record of the right
  # sibling page.
  #
  # However, we do know that the first leaf page is the correct leaf page for
  # the search key, because we used a b&#x2011;tree descent to find the page. That is
  # unambiguous. When we search for the search key in the leaf page, even if it
  # does not exist, and is greater than the last record, we still know for
  # certain that the key does not belong to any of the right siblings.
  #
  # The challenge here is an interface challenge. How do we convey the range to
  # the application developer? We cannot expose the binary search, because the
  # results of a binary search for a missing key are unambiguous only for the
  # first leaf page, and only if the key is same search key that brought us to
  # the first page.
  #
  # We expose an `index` property that is the index of the least record that is
  # equal to or greater than the search key in the current page.
  # 
  # When we move to a right sibling leaf page, the index property is set to
  # point to the first undeleted record in the leaf page. A leaf page will
  # retain a delete first record until the next balance, perserving it because
  # the record key is used as key in the branch pages. The index is zero if
  # there is no ghost record, or one if there is a ghost record. It is never
  # more than one.
  # 
  # Use the page index to begin iteration over the records in the leaf page.
  #
  # Additional public properties of the `Iterator` are the `key` used to create
  # the iterator, `found` which indicates whether or not the record actually
  # exists in the b&#x2011;tree, `count` which is the number of leaf pages visited by
  # this iterator, and `exclusive` indicating whehter we are an iterator or
  # a mutator. This properties are read-only, so make sure you only read them.

  # 
  constructor: (@key, @index, @found, { io, page, first, exclusive }) ->
    @_page = page
    @_io = io
    @length = @_page.positions.length
    @count = 1
    @exclusive = exclusive
    @first = first

  # Get a the record at the given `index` from the current leaf page.
  get: (index, _) ->
    if not (@index <= index < @length)
      throw new Error "index out of bounds"

    @_io.stash(@_page, index, _).record

  # Go to the next leaf page. Returns true if there is a next, false if there
  # the current leaf page is the last leaf page.
  next: (_) ->
    if @_page.right > 0
      if @_next
        next = @_next
        @_next = null
      else
        next = @_io.lock @_page.right, @_exclusive, _
      @_io.unlock @_page
      @_page = next
      true
    else
      false

  indexOf: (key, _) ->
    @_io.find @_page, key, @_page.deleted, _

  # Unlock all leaf pages held by the iterator.
  unlock: ->
    @_io.unlock @_page
    @_io.unlock @_next if @_next

# ### Mutator

# A mutator is an iterator that can also edit leaf pages. It can delete records
# from the currently visit leaf page. It can insert records into the current
# leaf page, if the record belongs in the current leaf page. Like an `Iterator`
# it  moves bacross the leaf pages of an the b&#x2011;tree in ascending collation
# order. It has random access to the records in the page using an index into the
# array of records.
#
# #### Ambiguous Insert Locations
# 
# Like the ambiguous range starts above, insert locations for new records can be
# ambiguous if the binary search indicates that a record should be inserted at
# the end of the leaf page, after the current last record in the leaf page. If
# the insert location is after the last record, it could be the case that the
# record really belongs to a right sibling leaf page page.
#
# As in the case of ambiguous range starts, an insert location is unambiguous if
# the key is the search key used to locate the first page of the mutator. The
# key is determined to belong inside the leaf page by virtue of a desent of the
# b&#x2011;tree. That is unambiguous.
#
# Using the example of time series data again, the application developer may
# have an hour of events she wishes to insert into a day of events, within a
# b&#x2011;tree of years of events. Because the events took place within an hour, they
# would all be inserted into leaf pages that are close to each other, perhaps
# right next to each other, or maybe they all fall within a single leaf page.
# 
# Rather than inserting an event at a time, desecnding the tree for each event,
# the application developer can create a mutator uses the earliest event as a
# key. This will give her the correct page for that first event. She can then
# insert the remainder of the events in their ascending order, while moving
# forward with the mutator in ascending order. If an event does not belong in
# the current leaf page, she can move the mutator forward to the next leaf page
# and see if it belongs there.
#
# The problem is that, after inserting that first record, if we determine that
# the  insert location for a subsequent record is after the last record on the
# current page, then it could be the case that it really belongs in in some
# right sibling page. If the first key of the next sibling page is less than or
# equal to the insert key, then we are on the wrong page. Unless we inspect the
# next page, the insert location is ambiguous.
#
# To obtain an unambiguous insert location without descending the b&#x2011;tree to
# create a new mutator, we add a `Mutator.peek` method.
#
# This gives the mutator permission to peek at the right sibling leaf page of
# the current leaf page, to determine if a record whose insert location is after
# the last record of the current page.
#
# If you do not grant the mutator permission to peek, then you cannot use the
# mutator to insert records into subsequent pages. You have to grant permission
# to peek for each page.
# 
# Why would you want to deny permission to peek when you're inserting a set of
# records? Maybe you're not certain that the records are close togther.
#
# Let's say our application developer wants to insert a set of events, but only
# a few of those events occured at or about the same time. They are for the most
# part in completely different months of the year.
#
# She might take the opportunity to see if the next event belongs on the current
# leaf page, but if it is actually a month away, the insert location will
# definately be after the last record in the page. She might not want to load
# the next page to resolve the ambiguity, when it is not likely to be resovled
# that the currnet leaf page is the correct leaf page.  Instead, she creates a
# new mutator to resolve the ambiguity, even though there is a slight chance
# she'll come back to the same page.
#
# If she is only inserting a single record, there's no ambiguity, because she'll
# use the key of the record to create the mutator. There is no need to enable
# peek for a single insert, but there is no real cost either.
#
# TODO The lock must held because deleting the record definately can change
# during editing. The balancer can swoop in and prune the dead first records and
# thereby change the key. It could not delete the page nor merge the page, but
# it can prune dead first records.
#
# The lock is held becase the first record of the next sibling might otherwise
# change, it might be deleted. When the first key is deleted, the range of the
# the keys that valid for the current page increases. We have a race condition
# where we might reject an insertion into the current record because it is
# less than the first key of the next, but the first key of the next page has
# been deleted, and the current record is less than the range of the new first
# key, it is within the extended range.
#
# duplicate keys: Duplicate keys again. Now it occurs to me that duplicates are
# actually not difficult for the application developer to implement given a
# mutator. The application developer can move forward through a series and
# append a record that has one greater than the maximum record.  Not a problem
# to worry about ambiguity in this case. Ah, we need to peek though, because we
# need to get that number.
#
# In fact, given a key plus a maximum series value, you will always land after
# the last one the series, or else a record that is less than the key, which
# means that the series is zero. Deleted keys present a problem, so we need to
# expose a leaf page key to the user, which, in case of a delete, is definately
# the greatest in the series.
#
# TODO Zero is a valid index for the left most leaf page.

#
class Mutator extends Iterator
  peek: ->
    peeking = @_peek is @_count
    @_peek = @_count
    peeking

  insert: (object, _) ->
    if object instanceof Cassette
      { key, record } = object
    else
      [ record, key ]  = [ object, @_io.extractor object ]

    # If we are at the first page and the key is equal to the key that created
    # the mutator, than this is the correct leaf page for the record.
    unambiguous = @count is 1 and @key and @_io.comparator(@key, key) is 0

    if unambiguous
      [ index, found ] = [ @index, @found ]
    else
      index = @_io.find page, key, page.deleted, _
      unless found = (index >= 0)
        index = ~index
      unambiguous = index <= page.count

    if found
      # TODO Special case: adding a first second copy of a record that has been
      # deleted, but still exists. We put the new record after the first record
      # and it exists as a special kind of duplicate.
      # TODO Think harder about this. It seems like it would work.
      # TODO No, we're not including the `0` in the range.
      index = ~index

    if index is 0 and not @first
      throw new Error "lesser key"
    else if index >= 0
      if not unambiguous and @_peek is @_count
        unless unambiguous = page.next is -1
          @_next or= @_io.lock @_page.right, @_exclusive, true, _
          unambiguous = @_io.compare(key, @_io.key(@_next, 0, _)) < 0

      if unambiguous
        # Cache the current count.
        @_io.balancer.unbalanced(@_page)

        # Since we need to fsync anyway, we open the file and and close the file
        # when we append a JSON object to it. Because no file handles are kept
        # open, the b&#x2011;tree object can left to garbage collection.
        filename = @_io.filename @_page.address
        fd = fs.open filename, "a", 0644, _
        position = @_io.writeInsert fd, @_page, index, record, _
        fs.close fd, _

        @_page.positions.splice index, 0, position
        @_page.count++
        @length = @_page.positions.length
      else
        index = 0
    index

  delete: (index, _) ->
    if not (0 <= index < @_page.count)
      throw new Error "index out of bounds"

    @_io.balancer.unbalanced(@_page)

    filename = @_io.filename @_page.address
    fd = fs.open filename, "a", 0644, _
    position = @_io.writeDelete fd, @_page, index, index is 0 and not @first, _
    fs.close fd, _
    
    @_page.positions.splice index, 1 if index > 0 or @first
    @_page.count--
    @length = @_page.positions.length
#
# #### Insertion and Deletion Verus Balance
#
# We do not attempt to balance the tree with every insertion or deletion. The
# client may obtain a cursor to the leaf pages, iterate through them deleting
# records along the way. Balacing 
#
# #### Staccato Blanace Operations
#
# The b&#x2011;tree balance operations cascade by nature. If you insert a value into a
# leaf node, such that the leaf node is beyond capacity, you split the leaf
# node, adding a new child to the parent node. If the parent node is now beyond
# capacity, you split the parent node, adding a new child to its parent node.
# When every node on the path to the leaf node is at capacity, a split of the
# leaf node will split every node all they way up to the root.
#
# Merges too move from leaves to root, so that a merge at one level of the
# b&#x2011;tree potentially triggers a merge of the parent with one of its siblings.
#
# However, we've established rules for lock acquisition that require that locks
# are obtained from the top down, and never from the bottom up. This is why we
# do not perform balance operations as a part of a single pass. We instead
# descend the tree once to insert or delete records form the leaf pages. We then
# descend the tree once for each split or merge of a page.
#
# Much b&#x2011;tree literature makes mention of a potential efficency where you split
# full pages on the way back up from an insert. You can determine which pages
# would split if the leaf split as you descend the b&#x2011;tree, since you'll visit
# every page that would participate in a split.
#
# That efficency applies only for split, and not for merge, because you have to
# inspect the left and right siblings of a page to determine if it is time to
# merge. If the left sibling page of a page, is not also child of that page's
# parent page, then the left sibling page is in a different sub&#x2011;tree. It can not
# be reached by the path that was used to find the leaf page where the delete
# occured.
#
# The single pass insert on the way down and split on the way up violates the
# rules we laid out to prevent deadlock. To abide by our rules, we'd have to
# lock exclusively on the way down, then hold the locks on the pages above the
# leaf page that were full and could possibly split. This would reduce the
# liveliness of our implementation.
#
# There are compromises, but rather than create a complicated locking apparatus,
# with upgrades, we're going to simplify our algorithm greatly, by descending
# the tree once for each split or merge. 
#
# When we travel to the unbalanced page, we acquire shared locks in the hand
# over hand fashion used for search. We acquire exclusive locks only on those
# pages that participate in the balance operation. That is two pages in the case
# of the split. In the case of a merge that is three pages. During a balance
# operation are locking exclusively, at most, three pages at a time.
#
# If out balance operation casades so that it requires a balance at every level,
# we'll descend the tree once for every level. However, the path we follow is
# almost certain to be in memory, since we're revisting the same path.
#
# Also, a balance operation will involve an increasing number of levels with
# decreasing frequency. A split will most often require that only the leaf page
# is split. The penultimate pages will be involved in a balance operation at
# least an order of mangitude less frequently. The pages above the penultimate
# branch pages will be involved in a balance operation yet another order of
# mangniutde less frequently.
#
# Conserving descents during balance operations is a false economy. It
# complicates lock acquisition. It reduces the liveliness of the b&#x2011;tree.
#
# The multiple descents will allow searches of the b&#x2011;tree to make progress
# between balance operations.
#
# ##### Descent as Unit of Work
#
# We can see that a descent of the tree is analogous to a thread in
# multi-threaded operation. A decent is an actor on the tree, performing a
# single balance operation, searching 
#
# ##### Delayed Balance
#
# We've determined that we do not want to perform a split or merge of the leaf
# level the moment we've detected the need for one. If we fill a leaf page, we
# descend the tree again to split the leaf page.
#
# Because the b&#x2011;tree is a concurrent structure, the leaf split descent may
# discover that another descent has removed a record, and a leaf split is no
# longer necessary. There may be, in fact, a descent on the way to the left
# sibling of the page, to check for the potential for a merge.
#
# The concurrent operation means that we have to deal with situation where we've
# undertaken a descent to balance the b&#x2011;tree, but another series of descents
# has rendered that plan invalid.
#
# As long as we're dealing with that problem, we may as well decouple insertion
# and deletion form split and merge entirely, and see if we can't gain more
# liveliness, and a simpiler implementation, by separating these concerns.
#
# We can provide an interface where the application developer can insert or
# delete any number of records, then order a balance of the tree that takes all
# the changes into account. This can avoid degenerate cases of sort where a leaf
# page at the split threshold and the application in turn inserts and deletes a
# single record from it.
#
# We can provide the application developer with a cursor. The cursor can delete
# a range of values, or insert a range of values, without having to descend the
# tree for each inserted value. The developer can insert or delete records as
# fast as the operating system can append a string to a file. We'll balance this
# mess for her later.
#
# It is still cheap to check for balance for single inserts or deletes, as if we
# were checking as part of a single pass.
#
# ##### Balance Thread
#
# We perform balance operations one a time. We do not begin a new balance
# operation until the previous one completes. In essence, we perform balancing
# in a separate thread of execution.
#
# Of course, a Node.js application really only has one thread of execution. In
# our b&#x2011;tree, however, multiple descents can make progress at the time, or
# rather, the progress made by one decent, while another descent waits on I/O.
#
# We ensure that only one descent at a time is making progress toward the
# balance of the tree. This simpilies or balance implementation, because the
# structure of the tree, its depth and number of pages, will only be changed one
# one series of descents. A balance descent can assume that the tree retain its
# structure while the balance descent waits on I/O.
#
# ##### Balancer
#
# We will call the code that co-ordinates the series of splits and merges to
# balance the tree, the *balancer*.
#
# The balancer maintains an offset count for each modified leaf page in the
# b&#x2011;tree. When an insert is performed, the offset count for the leaf page is
# incremented.  When a delete is performed, the offset count for the leaf page
# is decremented. This keeps track of the total change in size.
#
# We use the offset counts to determine which pages changed.
#
# ##### Balance Cutoff
#
# When it comes time to balance, we create a new balancer and put it in place to
# collect a new round of offset counts, while we are off balancing the pages
# gathered in the last round balance counts.
#
# We create a balance plan for the current set of pages. We balance the tree,
# splitting and merging pages according to our balance plan. Only one balancer
# balances the tree at a time. The balancer perform one split or merge descent
# at a time. Balance descents are never concurrent with other balance descents.
#
# While balancing takes place, records can be inserted and deleted concurrently.
# Those changes will be reflected when the next balancer balances the tree.
#
# ##### Creating a Balance Plan
#
# When it comes time to balance, we consult the pages for which we've maintained
# the offset counts. If the page is greater than the maximum page size, we split
# the page. That much is obvious. Otherwise, if the offset count is negative,
# we've deleted records. There may be an opportunity to merge, so we check the
# left and right siblings of the page to determine if a merge is possible.
#
# Determining a plan for a merge requires inspecting three pages, the page that
# decreased in size, plus its left and right sibling pages. We merge a page with
# the sibling that will create the smallest page that is less than or equal to
# the maximum page size.
#
# ##### Purging the Cache
#
# When inspecting page for merge, we are only interested in the count of records
# in the page. It may have been a long time since the last merge, so we might
# have accumulated a lot of pages that need to be consulted. To create a
# complete plan, we'll need to gather up the sizes of all the leaf pages, and
# the only way to get the size of a page is to load it into memory. But, we
# won't need to keep the page in memory, because we only need the size.
#
# When we calculate a merge, we load the left and right sibling. We're touching
# a lot of pages that we don't really know that we need.
#
# When we lock a page, we indicate that if the page is loaded, it ought to be
# loaded into balancer most-recently used list. This indicates that the page was
# loaded by the balancer. We also set the balancer flag, indiciating that we
# need to preserve the page for record count, even if the actual page data is
# discarded by a cache purge.
#
# We check the cache size frequently. If we're going over, we offer up the
# balancer pages to the cache purge first. If we determine that a page will be
# used in a balance operation we add it to the core most-recently used list,
# where it is subject to the same priority as any other page loaded by the
# cache.
#
# ### Splitting
#
# Splitting is the simpiler of the two balancing operations.
#
# To split a leaf page, we start by obtaining the key value of the first record.
# We can do this by acquiring a read lock on the page, without performing a
# descent. The balancer gets to break some rules since it knows that we know
# that the b&#x2011;tree is not being otherwise balanced.
#
# We descend the tree until we encoutner the penultimate branch page that is the
# parent of the branch page.  We acquire an excsluive lock the branch page. We
# can release our shared lock and acquire an exclusive lock. We do not have
# retest conditions after the upgrade, because only the balancer would change
# the keys in a branch page, and we're the balancer.
#
# We allocate a new leaf page. We append the greater half of the record to the
# page. We add the page as a child to the penultimate branch page to the right
# of the page we've split. We unlock the pages.
#
# We can see immediately if the penultimate page needs to be split. If it does,
# we descend the tree with the same key, stopping at the parent of the
# penultimate page. We will have kept the address of the parent of the
# penultimate page for this reason. We split the penultimate page, copying the
# addresses to a new right sibling. Adding the right sibling to the parent. We
# make sure to clear the cache of keys for the addresses we are removing. (Oh,
# and clear the cache for the records when you leaf split. Oh, hey, copy the
# records as well, duh.)
#
# Note that a page split means a change in size downward. It means that one or
# both of our two halves may be a candidate to merge with what were the left and
# right siblings of the page before it split. There may have been less than half
# a page of records one or both both of the sides of the tree. After we split,
# we check for a chance to merge. More live lock fears, but one heck of a
# balanced tree.
#
# ### Need to Add
#
# We always balance cascading up. Unlike leaf pages, which we can allow to
# depleate, the branch pages cannot be allowed to become empty as we merge leaf
# pages. As we delete records, we still keep a ghost of a key. As we delete leaf
# pages, we delete the ghost keys. Branch pages become paths to nowhere. They
# don't hold their own keys, so they can't find them.  We'd have to have null
# keys in our tree. Even if we kept keys around, we're sending searches down a
# path to nowhere. There is no leaf page to visit. We get rid of these paths. We
# always balance the upper levels immediately, we perform the cascade. Our tree
# descent logic would have to account for these empty sub&#x2011;trees. Much better to
# balance and keep things orderly.
# 
# This raises a concerns about live lock, that we might be balancing 
#
# TK Yes file times are enough. Even if the system clock changes drastically,
# the file times are all relative to one another. It it changes during
# operation, that is a problem, but we're not going to endeavor to provide a
# solution that deals with erratic clock times. Worst case, how do we not detect
# a file in need of recovery? We ignore files older than the timestamp file. So,
# we might have the system clock move far backward, so that the timestamp file
# is much newer than all the files that are being updated. Oh, well. What's the
# big deal then? How do we fix that? If it is a server install, we demand that
# you maintain your clock. If it is a desktop install, we can comb the entire
# database, because how big is it going to be?
#
# Hmm... What are you going to do? This is why people like servers.
#
# ### Merging
#
# TODO Great example floating around. Imagine that you've implemented MVCC.
# You're always appending, until it is time to vacuum. When you vacuum, you're
# deleting all over the place. You may as well do a table scan. You might choose
# to iterate through the leaf pages.  You may have kept track of where records
# have been stored since the last vacuum, so if you have a terabyte large table,
# you're only vacuum the pages that need it.
#
# Merge is from right to left. When we merge we always merge a page into its
# left sibling. If we've determined that a page from which records have been
# deleted is supposed to merge with its right sibling, we apply our merge
# algorithm to the right sibling, so that is is merged with its left sibling. 
#
# When we compare a page from which records have been deleted against its
# siblings, if the left sibling is to be merged, we use the page itself, the
# middle page in our comparison. If the middle page is to merge with the right
# page, we use the right page.
#
# To merge a leaf page, we start by obtaining the key value of the first record.
# We can do this by acquiring a read lock on the page, without performing a
# descent. The balancer gets to break some rules since it knows that we know
# that the b&#x2011;tree is not being otherwise balanced.
#
# With that key, we descend the b&#x2011;tree. We know that the key value cannot
# change, because it is the balancer that alters keys. The first record may be
# deleted by editing, but a ghost of the first record is preserved for the key
# value, which is the key value of the page.
#
# When we encouter the key value in a branch page, we acquire an excsluive lock
# the branch page. We can release our shared lock and acquire an exclusive lock.
# We do not have retest conditions after the upgrade, because only the balancer
# would change the keys in a branch page, and we're the balancer.
#
# We then descend the child to the left of the key, instead of to the right as
# we would ordinarly. We descend to the left child acquiring, an exclusive lock,
# but retaining our exclusive lock on branch page where we found our key. We
# then descend to the right most child of every child page, acqcuiring exclusive
# locks in the hand-over-hand fashion, until we reach a leaf page. We are now at
# the left sibling of the page we want to merge.
#
# We've locked the branch page that contains the key exclusively so that we can
# reassign the key. It will no longer be valid when the page is merged into its
# left sibling because the first record is now somewhere in the midst of the
# left sibling. We lock excluslively hand-over-hand thereafter to squeeze out
# any shared locks. Our exclusive lock on the parent containing the key prevents
# another descent from entering the sub&#x2011;tree where we are performing the merge.
#
# We now proceed down the path to the merge page as we would ordinarily, except
# that we acquire exclusive locks hand-over-hand instead of shared locks. This
# will squeeze out any other descents.
#
# We retain the exclusive lock on the penultimate branch page. No other descent
# will be able to visit this penultimate branch, because we've blocked entry
# into the sub&#x2011;tree and squeeed out the other descents. We still need to hold
# onto the exclusive lock, however, otherwise the page might be discarded during
# a cache purge, which can happen concurently.
#
# We append the records in the merge page to its left sibling. We remove the
# address of the merged page from the penultimate page.
#
# If we've deleted the first child of the penultimate branch page, then we
# delete the cached key of the new first child. The new first child is the
# left-most child of the penultimate page. Its key, if it not the left-most page
# of the entire tree, has been elevated to the exclusively locked branch page
# where we ecountered the merge page key. We don't want keys to gather where
# they are not used. That is a memory leak.
#
# We clear the merge key from the branch page where we found it. The next
# descent that needs it will look up the new merge key. If we found the merge
# key in a penultimate page, we need to make sure to clear the key using the
# page address we stashed, because the page is now deleted.
#
# #### Merging Parent Branches
#
# Once we've merged a leaf page, we check to see if the penultimate branch page
# that lost a child can be merged with one of its siblings. The procedure for
# merging branch pages other than the root branch page is the same regardless of
# the depth of the branch page.
#
# We acquire the key for the left most page in the sub&#x2011;tree underneath the
# branch page. We do this by following the left most children until we reach a
# leaf page. We use that key to descend the tree.
#
# we lock the page exclusively. We retain that lock
# for the duration of the merge.
#
# When we encounter the key, we descent the child to the left of the key, then
# we decent the right most child of every page until we reach the page at the
# same depth as the merge page. That is the left sibling of the merge page.
#
# We can then obtain a size for the left sibling, the merge and the right
# sibling of the merge. If we are able to merge, we choose a merge page to merge
# that page into the left sibling.
#
# We now descend the tree with the key for the merge page. We lock that page
# exclusively. We go left then right to the level of the left sibling. We go
# right then left to reach the merge page. (We're using page addresses to know
# that we've reached the merge page, the key is going to only be useful to find
# path on takes to find the left sibling.) We can then copy append addresses to
# the left sibling. Remove the merge sibling from its parent. Delete the key
# from where we found it, so it can be looked up again.
#
# Before we lose track of the sub&#x2011;tree we're in, we descend to the poteinally
# new left most leaf of the parent, and obtain its key to repeat the process.
#
# #### Filling the Root Page
#
# If the parent is the root page, we only do something special when the root
# page reaches one child and no keys. At that point, the one child becomes the
# root. We will have deleted the last key, merged everything to the left.
#
# We descend again, locking the root. We lock the one child of the root. We copy
# the contents of the one root child into the root and delete the child.
#
# This will decrease the height of the b&#x2011;tree by one.
#
# #### Deleting Pages
#
# A page deletion is simply a merge where we prefer to use the empty page as the
# merge page. We have to make an exception when the empty page is the left most
# page in the entire tree, which is not uncommon for time series data where the
# oldest records are regularly purged.
# 
# #### Purging Deleted First Keys
#
# Now we merge the parents. We find a penultimate page by the key of the left
# most leaf. Similar go left, then get the up to three pages. See if they will
# fit. Keep them around in cache. You may visit them again soon.
#
# If they fit, then you merge them. Lock exclusively the form page where you
# found the key. Move left into the right. Rewriting. Remove the key from the
# parent. You will have it locked. Delete the key.
#
# Then, get the left most leaf key. Okay there is only one thread balancing, so
# we will have a consistant depth. This is merge.
#
# Delete the key to trigger the lookup.
#
# #### Deleted First Keys
#
# We fix deleted first keys at balance. Descend locking the key when we see it.
# Mark it deleted forever. Then delete the key. It will get looked up again.
#
# We split recursively. Split and put into the parent. If the parent is ready to
# split, descend the tree locking the parent of the parent exclusively. We'll
# track where that is. No one else is changing the height of the tree. Only one
# thread is changing the height of the tree.
#
# We will create a plan to merge and split. We execute our plan. If we reach a
# place where our plan is invalid, we requeue, but only if it is invalid. If it
# is not invalid, only different, we continue with our merge, or our split.
#
# This give us a potential for live lock. We're constantly creating plans to
# merge, that are invalidated, so we create plans and those are invalidated.
#
# Our descent of the tree to balance will be evented anyway. We can probably
# make our calculation evented. We were fretting that we'd exacerbate the live
# lock problem. Live lock is a problem if it is a problem. There is no real
# gauntlet to run. The user can determine if balance will live lock. If there
# are a great many operations, then balance, wait a while, then balance, wait a
# while. It is up the the end user.
#
# The end user can use the b&#x2011;tree a map, tucking in values, getting them out.
# Or else, as an index, to scan, perform table scans. We'll figure that out.
#
# Now I have an API problem. The client will have to know about pages to work
# with them. We can iterate through them, in a table scan. We can implement a
# merge. We probably need an intelligent cursor, or a reporting cursor.

# There has been a desire second guess the most-recently used list. There is a
# desire to link nodes to the back of the list or suspend purges.
#
# There is a desire to cache the addresses of the left sibling pages when
# possible, so I wouldn't have to descend the tree. Except that the
# most-recently used list works hand in hand with b&#x2011;tree descent. The higher
# levels of the tree are kept in memory, because they are more frequently visted
# than lower levels. To much iteration along one level threatens to purge other
# levels.
#
# One can imagine that when balancing b&#x2011;tree that has been left unbalanced for a
# long time, reading in many unbalanced leaf pages will cause the first ones to
# flush, which is a pity, since we'll probably need one of them.
#
# Perhaps we suspect a page needs to be split but it doesn't. If the balancer
# was the one to load the page, simply to determine that nothing needs to be
# done, their is a desire to expedite the removal of the page from the cache.
#
# There are so many desires. It makes one giddy with thoughts of premature
# optimization.
#
# We're going to descent the tree to find our left sibling to exercise the
# most-recently used cache. We are not going to second guess it. We're going to
# defer to the algorithms. The simpiler the code, the more trust you can have in
# the code, the more likely your code will be adopted. A wide user base can
# inform decisions on optimization. There is always a core of what your
# application needs to do, and Strata needs to search and edit records.
#
# Balancing the tree is maintainence. The balancer can take its time.

class Balancer
  constructor: ->
    @referenced = {}
    @counts = {}
    @operations = []

  unbalanced: (page) ->
    @counts[page.address] = page.count unless @counts[page.address]?

  # TODO If it is not exposed to the user, I don't underbar it.
  reference: (page) ->
    if not @referenced[page.address]
      @referenced[page.address] = page
      page.balancers++

  # TODO You will have to launch this in a worker thread, otherwise there is no
  # good way for you to handle the error, or rather, you're going to have to
  # have some form of error callback, which is a pattern I'd like to avoid.

  # TODO Once loaded, and marked as part of the balancer, we can do our
  # calculations in one fell soop. This triggers the consternation over what all
  # these extranious pages do to the cache.

  # Ah, also, when we do load these, when we want to get them from the cache, we
  # don't really need them to be loaded. We should reach in a probe the cache
  # ourselves. My former Java self would have to spend three days thinking about
  # encapsulation, probably create a new sub-project. Agony.

  # No. Race condition. We want to gather all the pages in memory, so we can
  # evaluate them, without someone else changing them. We add a page because it
  # has grown, but then, when we imagine that we've gathered all the pages
  # necessary, it turns out that in the mean time, that page has shrunk so that
  # it is now too small. We could create an outer loop that keeps on referencing
  # cache entries until all are available, but then you have an outer strange
  # condition where you might load the entire b&#x2011;tree, because you're taking so
  # long, and every page is being touched. Allow balance to be imperfect.
  balance: (@io, _) ->
    # We only ever run one balance at a time. If there is a balance in progress,
    # we do not proceed. We do note that a subsequent balance has been
    # requested, so that we can continue to balance.
    return if @balancing

    # We do not proceed if there is nothing to consider.
    addresses = Object.keys @counts
    return if addresses.length is 0

    max = @io.options.leafSize
 
    # We put a new balancer in place of the current balancer. Any edits will be
    # considered by the next balancer.
    @io.balancer = new Balancer
    @io.balancer.balancing = true

    # Prior to calculating a balance plan, we gather the sizes of each leaf page
    # into memory. We can then make a balance plan based on page sizes that will
    # not change while we are considering them in our plan. However, page size may
    # change between gathering and planning, and page size may change between
    # planning and executing the plan. Staggering gathering, planning and
    # executing the balance gives us the ability to detect the changes in page
    # size. When we detect that we can't make an informed decsision on a page,
    # we pass it onto the next balancer for consideration at the next balance.
    
    # For each page that has changed we add it to a doubly linked list.
    ordered = {}
    for address in addresses
      # Convert the address back to an integer.
      address = parseInt address, 10
      count = @counts[address]

      # We create linked lists that contain the leaf pages we're considering in
      # our blanace plan. This is apart from the most-recently used list that
      # the pages themselves form.
      # 
      # The linked list nodes contain a reference to the page, plus a reference
      # to the node containing the previous sibling, and a reference to the node
      # containing the next sibling. If a sibling is not participating in our
      # balance plan, then its link is null. This gives us one or more linked
      # lists that reference a series of leaf pages ordered according to their
      # order in the b&#x2011;tree.
      # 
      # We are always allowed to get a lock on a single page, so long as we're
      # holding no other locks.
      if not node = ordered[address]
        page = @io.lock address, false, _
        @reference(page)
        node = { page, key: @io.key(page, 0, _)  }
        @io.unlock page
        ordered[page.address] = node

      # If the page has shrunk in size, we gather the size of the left sibling
      # page and the right sibling page. The right sibling page 
      if node.page.count - count < 0
        if page.address isnt -1
          if not node.left
            descent = new Descent(@_io)
            next = descent.key(node.key)
            descent.descend(descend.key(node.key), descent.found(key))
            # TODO You know that this would drive you mad and cost you three
            # days if you were a Java programmer. Ecapsulation! Encapsulation!
            descent.index--
            descent.descend(descend.right(), descent.leaf())
            # Check to make sure we don't already have a node for the page.
            left = { page: descent.page, key: @io.key(descent.page, 0, _) }
            @reference(left.page)
            @io.unlock left.page

            ordered[left.page.address] = left

            left.right = node
            node.left = left
        if not node.right and page.right isnt -1
          if not right = ordered[page.right]
            ordred[page.right] = right = {}
            right.page = @io.lock address, false, _
            @reference(right.page)
            node = { page: right.page, key: @io.key(right.page, 0, _)  }
            @io.unlock right.page
          node.right = right
          right.left = node
      # Save us the trouble of possibly going left for a future count, if we
      # have an opportunity to make that link from the right free of a descent.
      else if not node.right and right =ordered[page.right]
        node.right = right
        right.left = node

    # The remainder of the calcuations will not be interrupted by evented I/O.
    # Gather the current counts of each page into the node itself, so we adjust
    # the count based on the merges we schedule.
    for address, node of ordered
      node.count = node.page.count

    # Break the link to next right node and return it.
    unlink = (node) ->
      if node
        if right = node.right
          node.right = null
          right.left = null
      right

    # Link a node 
    link = (node, right) ->
      if right
        right.left = node
        node.right = right

    # Break the lists on the nodes that we plucked because we expected that they
    # would split. Check to see that they didn't go from growing to shrinking
    # while we were waiting evented I/O.
    for address, count in Object.keys @counts
      node = ordered[address]
      difference = node.count - count
      # If we've grown past capacity, split the leaf. Remove this page from its
      # list, because we know it cannot merge with its neighbors.
      if difference > 0 and node.page.count > max
        # Schedule the split.
        @operations.push method: "splitLeaf", address: node.page.address
        # Unlink this split node, so that we don't consider it when merging.
        unlink node.left
        unlink node
      # Lost a race condition. When we fetched pages, this page didn't need to
      # be tested for merge, so we didn't grab its siblings, but it does now.
      # We ask the next balancer to consider it as we found it.
      else if difference < 0 and not (node.left and node.right)
        @io.balancer.counts[node.page.address] = count

    # Now remove any node from our ordered collection that is not the left most,
    # so that we have a collection of heads of linked pages.
    for address in Object.keys ordered
      delete ordered[address] if ordered[address].left

    # We schedule merges, removing the nodes we merge and the nodes we can't
    # merge until the list of nodes to consider is empty.
    loop
      # We're done where there are no more nodes to consider.
      addresses = Object.keys ordered
      break if addresses.length is 0

      # Break the links between pages that cannot merge.
      for address in addresses
        node = ordered[address]
        while node.right
          if node.count + node.right.count > max
            node = unlink node
            ordered[node.address] = node
          else
            node = node.right

      # Merge the node to the right of each head node into the head node.
      for address in addresses
        node = ordered[address]
        # Schedule the merge.
        # After we schedule the merge, we increase the size of the head node and
        # link the head node to the right sibling of the right node.
        if node.right
          right = unlink node
          @operations.push
            method: "mergeLeaves"
            left: node.page.address
            right: right.page.address
          node.count += right.count
          link node, unlink right
        # Remove any lists containing only one node.
        else
          delete ordered[address]

    # Perform the operations to balance the b&#x2011;tree.
    for operation in @operations
      @[operation.method](operation, _)

    # Decrement the reference counts. TODO Why a count and not a boolean?
    for address, page of @referenced
      page.balancers--

  splitLeaf: ({ address }, _) ->
    say "SPLITTING #{address}"

  _splitLeaf: (branch, key, _) ->
    address = branch.addresses[@io.find(child, @key, 1, _)]
    @exclusive.push leaf = @io.lock address, true, true, _

    # See that nothing has changed since we last descended.
    { comparator: c, io, options: { leafSize: length } } = @
    return if leaves.addresses.length < @options.leafSize

    # We might have let things go for so long, that we're going to have to split
    # the page into more than two pages.
    partitions = Math.floor leaf.count / @options.leafSize
    while (partitions * @options.leafSize) <= leaves.addresses.length
      # Find a partition.
      partition = Math.floor leaves.addresses.length / (partitions + 1)
      key = io.key leaves, partition, _
      pivot = @io.find(child, key, _) + 1

      addresses = leaf.positions.splice(partition)
      right = io.allocateLeaves(addresses, leaves.right)

      leaves.right = right.address

      child.addresses.splice(pivot, 0, right.address)

      # Order of rewrites is to first write the pages out. Then delete the old
      # files. Then copy the new files. The presence of a new file without an
      # old file means to roll forward. The presence of new files each with and
      # old file means to roll back.
      #
      # TODO Split relink into unlink and rename.

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

class Cassette
  constructor: (@record, @key) ->

class exports.Strata
  # Construct the Strata from the options.
  constructor: (options) ->
    defaults =
      leafSize: 12
      branchSize: 12
      comparator: comparator
      extractor: extractor
    @_io = new IO options.directory, extend defaults, options

  create: (_) -> @_io.create(_)

  open: (_) -> @_io.open(_)

  close: (_) -> @_io.close(_)

  get: (key, callback) ->
    operation = method: "get"
    mutation = new Descent(@, null, key, operation)
    mutation.descend callback

  # TODO NULL keys? No! Keys have to unique. We already determined that. What
  # good is ONE and only ONE null key? Madness. How do you store nulls? Shiver.
  # Okay, so you create a pseudo null value, you'd have to choose one. You might
  # use, for a key, a map that says `{ key: null, ordinal: 1 }`. Cookbook it.
  _cursor: (key, exclusive, constructor, _) ->
    descent = new Descent(@_io)
    next = if key? then descent.key(key) else descent.leftMost
    descent.descend(next, descent.penultimate, _)
    descent.exclude() if exclusive
    descent.descend(next, descent.leaf, _)
    index = descent.page.deleted
    index = @_io.find(descent.page, key, index, _) if key?
    index = ~index if not (found = index >= 0)
    new constructor(key, index, found, descent)

  iterator: (splat..., callback) ->
    @_cursor splat.shift(), false, Iterator, callback

  mutator: (splat..., callback) ->
    @_cursor splat.shift(), true, Mutator, callback

  # Insert a single record into the tree.

  #
  insert: (record, _) ->
    cursor = @cursor @cassette(record), _
    cursor.insert(record)
    cursor.unlock()

  # Create a cassette to insert into b&#x2011;tree.
  cassette: (object) -> new Cassette(object, @_io.extractor(object))

  # Create an array of cassettes, sorted by the record key, from the array of
  # records.
  cassettes: (objects...) ->
    sorted = (@record(object) for object in objects)

    { comparator } = @_io
    sorted.sort (a, b) -> comparator(a.key, b.key)

    sorted

  balance: (_) -> @_io.balancer.balance(@_io, _)

# ## Glossary
# 
# * <a name="map">**map**</a> &mdash; A JavaScript Object that is used as a key
# value store. We use the term *map*, because the term *object* is ambiguous.
