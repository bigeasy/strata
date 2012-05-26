# A Streamline.js friendly evented I/O b&#x2011;tree for Node.js.
#
# TK Define ***least child page***.
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
# in&#x2011;memory cache when it can, so requests can be satisfied even when
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
# Strata is a b&#x2011;tree. A b&#x2011;tree is a database primitive. Using
# Strata, you can start to experiment with database designs of your own. You can
# use Strata to build an MVCC database table, like PostgreSQL. You can create
# Strata b&#x2011;trees to create indexes into structured data that is not
# already in a database, like monster log files. You can use Strata to store
# your data in what ever form of JSON suits you like a NoSQL database.
#
# As a bonus, Strata is two database primitives in one, because with a time
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
# database in itself. Before you fork and add features you feel are missing,
# please consult with me. Perhaps your ideas are better expressed as project
# that employs Strata, instead of to a patch to Strata itself.
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
# We refer to the actual number of child pages in a branch page or the actual
# number of records in a leaf page as the page ***length***.
#
# The term ***b&#x2011;tree*** itself may not be correct. There are different
# names for b&#x2011;tree that reflect the variations of implementation, but
# those distinctions have blurred over the years. Our implementation may be
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
# Leaf pages are linked in ascending order to simplify the implementation of
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

# Required Node.js libraries.
fs = require "fs"

# Copy values from one hash into another.
extend = (to, from) ->
  to[key] = value for key, value of from
  to

# Used for debugging. If you don't see them called in the code, it means the
# code is absolutely bug free.
die = (splat...) ->
  console.log.apply console, splat if splat.length
  process.exit 1
say = (splat...) -> console.log.apply console, splat
hit = (hash) -> say "# HIT #{hash}" if process.env.STRATA_COVERAGE

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
# cursor, it will eventually be collected by a cache purge. If the key is
# frequently consulted by queries as they descend the tree, the key will be
# retained. If the key is subset of a large record, purging the records and
# retaining the keys will reduce the in&#x2011;memory size of the b&#x2011;tree.
#
# Also, the comparator is pretty easily generalized, while the extractor is
# invariably specialized. You might have a single string comparator that you use
# with extractors specialized for different types of records.
#
# ### Default Collation
#
# You will almost certainly define your own extractors and comparators, but the
# b&#x2011;tree has a default that works for b&#x2011;tree that stores only
# JavaScript primitives.

# Default comparator for JavaScript primitives. You can use `a - b` for numbers.
# This works with both strings and numbers.
comparator = (a, b) ->
  if a < b then -1 else if a > b then 1 else 0

# Default extractor returns the value as whole for use as a key.
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
# ### Page I/O
#
# The `IO` class manages the reading and writing of leaf and branch pages to and
# from disk, page locking and page caching. It also implements the binary search
# we use to search the pages.

#
class IO
  # ### Checksummed Lines of JSON

  # Each page is stored in its own ***page file***. The page files are all kept
  # in a single directory. The directory is specified by the application
  # developer when the `Strata` object is constructed.
  #
  # Page files contain one or more JSON strings, one string per line. The JSON
  # strings are checksummed. The checksum follows the JSON string on the line.
  # The checksum is written as a hexadecimal number.
  #
  # The line based JSON format plays nice with traditional UNIX text utilities.
  #
  # A ***leaf page file*** acts as journal recording edit events. A JSON string
  # is appended to the leaf page file to record a record insert or delete.
  #
  # A ***branch page file*** contains a single JSON object stored on a single
  # line that contains the array of child page addresses.
  #
  # **TK**: Documentation damage. We're now calling a reference array a position
  # array in a leaf page and an address array in a branch page. Do we want to
  # consolidate to reference array? I'm beginning to think so. May actions on
  # this array are the same for both leaf pages and branch pages.
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
  # **TK**: Damage mentioned above ends here.
  #
  # We count on our JavaScript array implementation to be [clever about memory
  # usage](http://stackoverflow.com/questions/614126/why-is-array-push-sometimes-faster-than-arrayn-value/614255\#614255).

  # Set directory and extractor. Initialize the page cache and most-recently
  # used list.
  #
  # The checksum will become CRC 32 by default in future releases and
  # configurable to use any of the `crypto` module hashes or no checksum at all.
  constructor: (@directory, @options) ->
    { @extractor
    , @comparator } = @options

    @cache          = {}
    @mru            = { address: null }
    @mru.next       = @mru
    @mru.previous   = @mru
    @nextAddress    = 0
    @length         = 1024
    @balancer       = new Balancer
    @size           = 0

    @checksum       = switch hash = @options.checksum or "sha1"
      when "none" then -> 0
      else
        (m) -> require("crypto").createHash(hash).update(m).digest("hex")

  # #### Verifying Checksums

  # When we read a line from a branch page file or a leaf page file, we always
  # verify the checksum. We use the checksum algorithm specified in the `Strata`
  # constructor.
  #
  # In our branch page files and leaf page files, we store a JSON string one per
  # line. The checksum is written as a hexadecimal number following the JSON
  # string. We checksum the JSON string to and compare it to the stored
  # checksum.
  #
  # A hyphen stored in place of the hexadecimal indicates no checksum.

  #
  _readLine: (line) ->
    match = /^\s?(.*)\s((?:-|[\da-f]+))\s?$/i.exec line
    if not match
      throw new Error "corrupt line: cannot split line: #{line}"
    if match[2] isnt "-" and @checksum(match[1]) isnt match[2]
      throw new Error "corrupt line: invalid checksum"
    JSON.parse(match[1])

  # Pages are identified by an integer page address. The page address is a number
  # that is incremented as new pages are created. A page file has a file name that
  # includes the page address.  When we load a page, we first derive the file name
  # from the page address, then we load the file.
  #
  # **TK**: Rewrite once we've finalized journaled balancing.
  #
  # The `filename` method accepts a suffix, so that we can create replacement
  # files. Instead of overwriting an existing page file, we create a replacement
  # with the suffix `.new`. We then delete the existing file with the `delete`
  # method and move the replacement into place with the `replace` method. This
  # two step write is part of our crash recovery strategy.
  #
  # We always write out entire branch page files. Leaf pages files are updated
  # by appending, but on occasion we rewrite them to vacuum deleted records.

  # Create a file name for a given address with an optional suffix.
  filename: (address, suffix) ->
    address = Math.abs(address)
    suffix or= ""
    padding = "00000000".substring(0, 8 - String(address).length)
    "#{@directory}/segment#{padding}#{address}#{suffix}"

  # Move a replacement page file into place. Unlink the existing page file, if
  # it exists, then rename the replacement page file to the permanent name of
  # the page file.
  replace: (page, suffix, _) ->
    replacement = @filename(page.address, suffix)
    stat = fs.stat replacement, _
    if not stat.isFile()
      throw new Error "not a file"
    permanent = @filename(page.address)
    try
      fs.unlink permanent, _
    catch e
      throw e unless e.code is "ENOENT"
    fs.rename replacement, permanent, _

  # Rename a page file from a page file with one suffix to another suffix.
  rename: (page, from, to, _) ->
    fs.rename @filename(page.address, from),
              @filename(page.address, to), _

  # Unlink a page file with the given suffix.
  unlink: (page, suffix, _) ->
    fs.unlink @filename(page.address, suffix), _

  # ### Page Caching
  #
  # We keep an in&#x2011;memory map of page addresses to page objects. This is
  # our ***page cache***.
  #
  # #### Most-Recently Used List
  #
  # We also maintain a ***most-recently used list*** as a linked list using the
  # page objects as list nodes. When we reference a page, we unlink it from the
  # linked list and relink it at the head of the list. When we want to cull the
  # cache, we can remove the pages at the end of the linked list, since they are
  # the least recently used.
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

  # Link tier to the head of the most-recently used list.
  link: (head, entry) ->
    next = head.next
    entry.next = next
    next.previous = entry
    head.next = entry
    entry.previous = head
    entry

  # Unlink a tier from the most-recently used list.
  _unlink: (entry) ->
    { next, previous } = entry
    next.previous = previous
    previous.next = next
    entry

  # #### Cache Purge Trigger
  #
  # There are a few ways we could schedule a cache purge; elapsed time, after a
  # certain number of requests, when a reference count reaches zero, or when
  # when a limit is reached.
  #
  # We take the limits approach. The bulk of a cached page is the size of the
  # references array and the size of objects in records map. We keep track of
  # those sizes. When we reach an application developer specified maximum size
  # for cached records and page references for the entire b&#x2011;tree, we
  # trigger a cache purge to bring it below the maximum size. The purge will
  # remove entries from the end of the most-recently used list until the limit
  # is met.
  #
  # #### JSON Size
  #
  # There is no way to get the size of a JavaScript object in memory, so we
  # don't have a way to know the actual size of cached pages in memory.
  #
  # We can get a pretty good relative measure of the size of a page, however,
  # using the length of the JSON strings used to store records and references.
  #
  # The ***JSON size*** of a branch page is the string length of the address
  # array when serialized to JSON, plus the string length of each key loaded
  # into memory when serialized to JSON. The JSON size of leaf page is the
  # string length of the file position array when serialized to JSON, plus the
  # string length of each record loaded in memory when serialized to JSON.
  #
  # This is not an exact measure of the system memory committed to the in memory
  # representation of the b&#x2011;tree. It is a fuzzy measure of the relative
  # heft of page in memory. An exact measure is not necessary. We only need to
  # be sure to trigger a cache purge at some point before we reach the hard
  # limits imposed by system memory or the V8 JavaScript engine.
  #

  # Adjust the JSON size of the given page and the entire b&#x2011;tree.
  heft: (page, size) ->
    page.size += size
    @size += size

  # #### Pages Held for Housekeeping
  #
  # There may be page objects loaded for housekeeping only. When balancing the
  # tree, the length of a page is needed to determine if the page needs to be
  # split, or if it can merged with a sibling page.
  #
  # We only need the order of the page to create our balance plan, however, not
  # the cached references and records. The page object keeps a copy of the order
  # in a `order` property. We can delete the page's reference array, as well as
  # the page's object cache. The page object the page entry itself cannot be
  # removed from the cache until it is no longer needed to calculate a split or
  # merge.
  #
  # We use reference counting to determine if an entry is participating in
  # balance calculations. If the page is being referenced by a balancer, we
  # purge the reference array and the cached records and keys, but we do not
  # unlink the page object from the most-recently used list nor remove it from
  # the cache.

  # ### Leaf Pages
  #
  # Five key things to know about leaf pages.
  #
  # * A leaf page is an array of records.
  # * A leaf page cannot contain two records that share the same key, therefore
  # the b&#x2011;tree cannot contain duplicates.
  # * The key of the first record is the key for the leaf page, the keys of all
  # other records in the leaf page are always greater than the key for the leaf
  # page.
  # * If the first record is deleted, we keep a it as a ghost record, for the
  # sake of the key, until the leaf page can be vacuumed.
  # * The leaf page file is a text file of JSON strings that is an append log of
  # record insertions and deletions.
  #
  # #### Constant Time
  #
  # In the abstract, a leaf page is an array of records.  Given an integer, the
  # leaf page will return the record stored at the offset of the array. This
  # lookup is performed in constant time when the record is in memory.
  #
  # This lookup is performed in more or less constant time when the record is
  # uncached, so long you're willing to say that random access into a file is
  # constant time for our purposes. Otherwise, lookup is *O(log n)*, where *n*
  # is the number of file system blocks used to store the leaf page file.
  #
  # #### Binary Search
  #
  # Our leaf page implementation maintains an array of file positions called a
  # positions array. A file position in the positions array references a record
  # in the leaf page file by its file position. The positions in the positions
  # array are sorted according to the b&#x2011;tree collation of the referenced
  # records.
  #
  # In the leaf page file, a record is stored as JSON string. Not all of the
  # records are loaded when the page loads. Records that are not loaded when the
  # page is loaded are loaded as needed. The leaf page keeps a map (a JavaScript
  # `Object`) that maps file positions to deserialized records.
  #
  # Because the records are sorted, and because a lookup takes constant time, we
  # can search for a record in a leaf page using binary search in logarithmic
  # time.
  #
  # #### No Duplicates
  #
  # Leaf pages cannot contain duplicate records. Therefore, the b&#x2011;tree
  # cannot contain duplicate records.
  #
  # You can simulate duplicate records by adding a series value to your key and
  # which is stored in your record. The cursor implementation is designed
  # facilitate ***pseudo-duplicate*** keys in this fashion.
  #
  # In theory, leaf pages can contain `null`, and `null` can be used as a key.
  # However, if you want to allow `null` keys in your b&#x2011;tree, you almost
  # certainly want to allow more than one `null` key, so you'll end up using the
  # pseudo-duplicate strategy for `null` keys as well.
  #
  # #### Ghosts and Leaf Page Length
  #
  # When we delete the first record of a leaf page, we keep the first record
  # around, because its key value is the key value for the leaf page. Changing
  # the key of a leaf page requries re-balancing the tree, so we need to wait
  # until we balance to vacuum the deleted first record.
  #
  # When we delete the first record we increment the `ghosts` property of the
  # page by `1`. The acutal length of a leaf page is the value `length` less the
  # value of the `ghosts` property. Only the first record is ever turned into a
  # ghost if deleted, so the value `ghosts` property is only ever `0` or `1`.
  #
  # #### Leaf Page Split
  #
  # If the length of a leaf page exceeds the leaf page order, the leaf page is
  # split when the b&#x2011;tree is balanced.

  # The in memory representation of the leaf page includes the address of the
  # leaf page, the page address of the next leaf page, and a cache that maps
  # record file positions to records that have been loaded from the file.
  createLeaf: (address, override) ->
    page =
      address: address
      balancers: 0
      cache: {}
      entries: 0
      length: 0
      locks: [[]]
      ghosts: 0
      positions: []
      right: 0
      size: 0
    extend page, override or {}

  # Add a leaf page or a branch page to the page cache and link it to the head
  # of the most-recently used list.
  encache: (page) ->
    @cache[page.address] = @link @mru, page

  # #### JSON Leaf Page Size
  #
  # The JSON size of leaf page is the string length of the file position array
  # when serialized to JSON, plus the string length of each record loaded in
  # memory when serialized to JSON.
  #
  # ##### JSON Record Size
  #
  # We have to cache the calculated size of the record because we return the
  # records to the application developer. We're not strict about ownership, we
  # don't defensively copy the record before returning it or anything, so the
  # application developer may alter the record. When we uncache the record, we
  # won't be able to trust the recalculated JSON size. We keep a copy of the
  # size in an object in the leaf page cache.
  #
  # Each leaf page cache entry contains the record, key and the size of the
  # object at the time of caching.

  #
  cacheRecord: (page, position, record, key) ->
    # Uncache the exisiting record.
    @uncacheRecord page, position

    # Extract the key if none was provided.
    key ?= @extractor record

    # Create a cache entry.
    entry = page.cache[position] = { record, key }

    # Calculate the size.
    entry.size = JSON.stringify(entry).length

    # Increment the page size and the size of the b&#x2011;tree.
    @heft page, entry.size

    # Return our entry.
    entry

  # Delete a record from the leaf page cache. Deduct the cached JSON size of the
  # record entry from the size of the page and the size of b&#x2011;tree.
  uncacheRecord: (page, position) ->
    if size = page.cache[position]?.size
      @heft page, -size
      delete page.cache[position]

  # ### Appends for Durability
  #
  # A leaf page file contains JSON objects, one object on each line. The objects
  # represent record insertions and deletions, so that the leaf page file is
  # essentially a log. Each time we write to the log, we open and close the
  # file, so that the operating system will flush our writes to disk. This gives
  # us durability.
  #
  # The caller determines which file should be written, so it opens and closes
  # the file descriptor. For record insertions and deletions, a file descriptor
  # is opened and closed for a single append. When rewriting an existing leaf
  # page in order to compact it, the file descriptor is kept open for the
  # multiple appends of the rewrite.
  #
  # The file descriptor must be open for for append.

  #
  _writeJSON: (fd, page, object, _) ->
    page.position or= fs.fstat(fd, _).size

    # Format the line with checksums.
    json = JSON.stringify object
    line = "#{json} #{@checksum(json)}"

    # Calcuate a buffer length. Take note of the current page position.
    position        = page.position
    length          = Buffer.byteLength(line, "utf8")
    buffer          = new Buffer(length + 1)
    offset          = 0

    # Write JSON and newline.
    buffer.write line
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

  # ### Leaf Page Journal
  #
  # The leaf page acts as an edit journal recording edit events. Each event is
  # stored as a ***journal entry***. These journal entires are appended to the
  # leaf page files as JSON arrays, one JSON array per line in the file.
  #
  # There are three types of journal entires recorded in a leaf page; ***insert
  # entires***, ***delete entries***, and ***position array entries***.
  #
  # The insert and delete entries record changes to the the leaf page. Beginning
  # with an empty position array and reading from the start of the file, the
  # leaf tier is reconstituted by replaying the inserts and deletes described by
  # the insert and delete entries.
  #
  # Position array entries record the state of the position array later on in
  # the history of the leaf page file, so that we don't have to replay the
  # entire history of the leaf page file in order to load the leaf page.
  #
  # #### Position Array
  #
  # Each leaf page has a ***position array***. The position array references the
  # position in the leaf page file where an insert entry records the insertion
  # of a record into the leaf page. When we want the record, if it is not in
  # memory, then we read it from the leaf page file at the given file position.
  #
  # When the record has been read from the leaf page file, it is cached in the
  # `cache` object property of the in&#x2011;memory page object indexed by its
  # file position.
  #
  # When we write an insert entry, we take note of the insert entries file
  # position in the leaf page and use that position as its place holder in the
  # position array.
  #
  # The position array maintains the file positions of the inert entries in the
  # collation order of the b&#x2011;tree.
  #
  # #### Per-Entry Housekeeping
  #
  # Each entry include a count of entries in the leaf page. The count of entries
  # is always increasing by one. It is essentially a line number. We can detect
  # missing lines by detecting a break in the series. We perform this check when
  # loading a leaf page, to an extent. We can perform it against the entire leaf
  # page if we suspect corruption. **TIDY**
  #
  # Each insert or delete entry also includes the count of records in the leaf
  # page including the effects of the entry itself. The position array entry
  # includes the count of records implicitly, since it includes the position
  # array, which contains a position for each entry.
  #
  # **TODO** Including or prior to. I almost like prior to better. Almost easier
  # to document. No, prior to is easier to document, but then it becomes
  # inconsistent with entry number.
  #
  # #### Insert Entries
  #
  # We determine if an entry is an insert entry by examining the first element
  # in the entry JSON array.
  #
  # If the first element is an integer greater than zero, it indicates an insert
  # entry. The integer is the one based index into the zero based position
  # array, indicating the index where the position of the current insert object
  # should be inserted. The next two elements are the journal housekeeping. The
  # last element is of the insert entry is the record object.
  #
  # The JSON array elements form a structure as follows.
  #
  #  * One-based index into position array.
  #  * Count of records in leaf page including insert.
  #  * Count of entries in leaf page including insert.
  #  * Record to insert.
  #
  # When we read the insert entry, we will place the record in the record cache
  # for the page, mapping the position to the record.

  # Write an insert object.
  writeInsert: (fd, page, index, record, _) ->
    entry = [ index + 1, page.length - page.ghosts + 1, ++page.entries, record ]
    @_writeJSON fd, page, entry, _

  # #### Delete Entries
  #
  # If the first element of our entry is less than zero, it indicates a delete
  # entry. The absolute value of the integer is the one based index into the
  # zero based position array, indicating the index of the position array
  # element that should be deleted.
  #
  # The next two elements are the journal housekeeping. The last element is of
  # the insert entry is the record object.
  #
  # The JSON array elements of a delete entry form a structure as follows.
  #
  #  * Negated one-based index into position array.
  #  * Count of records in leaf page including delete.
  #  * Count of entries in leaf page including delete.
  #
  # Special handling of a deleted first record is required when we replay the
  # journal. The first record of a leaf page is not actually deleted from their
  # in-memory pages, but ghosted. We keep them around because the key of the
  # first record is the key for a page.
  #
  # There is no special accounting necessary to record the fact that the first
  # record is a ghost in the delete entry. We can see that it was the first
  # record that was deleted.
  #
  # There are no other elements in the JSON array for a delete entry, just the
  # negated one&#x2011;based index of the record to delete.

  # Write a delete object.
  writeDelete: (fd, page, index, _) ->
    entry = [ -(index + 1), page.length - page.ghosts - 1, ++page.entries ]
    @_writeJSON fd, page, entry, _

  # #### Position Array Entires
  #
  # A position array entry contains the position array itself. On occasion, we
  # store a copy of a constructed position array entry in the leaf page file so
  # that we can read a large leaf page file quickly.
  #
  # When we read a leaf page file, if we read from the back of the file toward
  # the front, we can read backward until we find a position array entry. Then
  # we can read forward to the end of the file, applying the inserts and deletes
  # that occurred after we wrote the position array entry.
  #
  # When a leaf page file is large, stashing the constructed position array at
  # the end means that the leaf page can be loaded quickly, because we will only
  # have to read backwards a few entries to find a mostly completed position
  # array. We can then read forward from the array to amend the position array
  # with the inserts and deletes that occurred after it was written.
  #
  # Not all of the records will be loaded when we go backwards, but we have
  # their file position from the position array, so we can jump to them and load
  # them as we need them. That is, if we need them, because owing to binary
  # search, we might only need a few records out of a great many records to find
  # the record we're looking for.
  #
  # The position array entry includes some constant properties of the leaf page.
  #
  # We write an array with a leaf page file format version number, indicating
  # the version of the leaf page file format, and therefore the version of
  # entire the b&#x2011;tree file format.
  #
  # We also include the address of the right sibling. This address will only
  # change when the leaf page file is rewritten.
  #
  # The JSON array elements of a delete entry form a structure as follows.
  #
  #  * Zero to indicate a position array entry.
  #  * Leaf page file format version number.
  #  * Address of the right sibling leaf page.
  #  * Count of ghost records, only ever `0` or `1`.
  #  * Count of entries in leaf page including insert.
  #  * The position array.
  #
  # The position array entry also acts as a header. We always place one at the
  # start of a leaf page, so that we can look at the head of the head of a leaf
  # page file to find its version and right sibling leaf page.
  #
  # **TK**: Counted b&#x2011;trees.

  # Write an position array entry.
  writePositions: (fd, page, _) ->
    entry = [ 0, 1, page.right, page.ghosts, ++page.entries, page.positions ]
    @_writeJSON fd, page, entry, _

  # #### Reading Leaves

  # Here is the backward search for a position in array in practice. We don't
  # really ever start from the beginning. The backwards than forwards read is
  # just as resilient.

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
    # Temporary cache of records read while searching for position array object.
    cache     = {}

    #
    line      = ""
    end       = stat.size
    eol       = stat.size
    buffer    = new Buffer(1024)
    while end
      end     = eol
      start   = end - buffer.length
      start   = 0 if start < 0
      read    = fs.read fd, buffer, 0, buffer.length, start, _
      end    -= read
      if buffer[--read] isnt 0x0A
        throw new Error "corrupt leaves: no newline at end of file"
      eos     = read
      while read != 0
        read = read - 1
        if buffer[read] is 0x0A or start is 0 and read is 0
          entry = @_readLine buffer.toString "utf8", read, eos
          eos   = read
          index = entry.shift()
          if index is 0
            entry.shift() # leaf page file format version
            page.right = entry.shift()
            page.ghosts = entry.shift()
            page.entries = entry.shift()
            positions = entry.shift()
            end = 0
            break
          else
            position = start + read + 1
            if index > 0
              cache[position] = entry.pop()
            splices.push [ index, position, entry.pop() ]
      eol = start + eos

    # Prime our page with the positions array read from the leaf file, or else
    # an empty positions array.
    @splice page, 0, 0, positions

    # Now we replay the inserts and deletes described by the insert and delete
    # objects that we've gathered up in our splices array.
    splices.reverse()
    for splice in splices
      [ index, position, entry ] = splice
      if entry isnt ++page.entries
        throw new Error "leaf corrupt: incorrect entry position"
      if index > 0
        @splice page, index - 1, 0, position
      else if ~index is 0 and page.address isnt -1
        if page.ghosts then throw new Error "double ghosts"
        page.ghosts++
      else
        @splice page, -(index + 1), 1

    # Cache the records we read while searching for the position array object.
    for position in page.positions
      if cache[position]
        @cacheRecord page, position, cache[position]

    # Close the file descriptor.
    fs.close fd, _

    # Return the loaded page.
    page

  # Each line is terminated by a newline. In case your concerned that this
  # simple search will mistake a byte inside a multi-byte character for a
  # newline, have a look at
  # [UTF-8](http://en.wikipedia.org/wiki/UTF-8#Description). All bytes
  # participating in a multi-byte character have their leading bit set, all
  # single-byte characters have their leading bit unset. Therefore, `"\n"` is
  # unambiguous.

  # Search for the newline that separates JSON records.
  _readJSON: (buffer, read) ->
    for offset in [0...read]
      if buffer[offset] is 0x0A
        return @_readLine buffer.toString("utf8", 0, offset + 1)

  # Our backward read can load a position array that has been written to the
  # leaf page file, without having to load all of the records referenced by the
  # position array. We will have to load the records as they are requested.
  #
  # To load a record, we open the file and jump to the position indicated by the
  # position array. We then read the insert object that introduced the record to
  # the leaf page file.
  #
  # We open a file descriptor and then close it after the record has been read.
  # The desire to cache the file descriptor is strong, but it would complicate
  # the shutdown of the b&#x2011;tree. As it stands, we can always simply let
  # the b&#x2011;tree succumb to the garbage collector, because we hold no other
  # system resources that need to be explicitly released.
  #
  # Note how we allow we keep track of the minimum buffer size that will
  # accommodate the largest possible buffer.
  #
  # **TODO**: Have a minimum buffer that we constantly reuse, uh no. That will
  # be shared by descents.

  #
  readRecord: (page, position, _) ->
    filename = @filename page.address
    page.position or= fs.stat(filename, _).size
    fd = fs.open filename, "r", _
    loop
      buffer = new Buffer(@length)
      read = fs.read fd, buffer, 0, buffer.length, position, _
      if entry = @_readJSON(buffer, read)
        break
      if @length > page.position - position
        throw new Error "cannot find end of record."
      @length += @length >>> 1
    fs.close fd, _
    entry.pop()

  # Over time, a leaf page file can grow fat with deleted records. Each deleted
  # record means there's an insert object that is no longer useful. The delete
  # record is only useful as a marker. We vacuum the leaf page file to get rid
  # of these pairs of objects that have negated each other.
  #
  # We vacuum a leaf page file by writing it to a replacement leaf page file,
  # then using `relink` to replace the current leaf page file with the
  # replacement.
  #
  # All of records referenced by the current position array are appended into
  # the replacement leaf page file using insert objects. A position array object
  # is appended to the end of the replacement leaf page file. The rewritten leaf
  # page file will load quickly, because the position array object will be found
  # immediately.

  # Note that we close the file descriptor before this function returns.
  rewriteLeaf: (page, suffix, _) ->
    filename = @filename page.address, suffix

    # Open the new leaf page file and reset our file position.
    fd = fs.open filename, "a", 0o644, _
    positions = []
    cache = {}
    page.position = 0
    page.entries = 0

    # Capture the positions, while truncating the page position array.
    positions = @splice page, 0, page.length

    # Write an empty positions array to act as a header.
    @writePositions fd, page, _

    # Rewrite each object in the positions array.
    for position, index in positions
      # Read the object from the current page, but then uncache it.
      object = @stash page, position, _
      @uncacheRecord page, position

      # Write the record to the new file.
      position = @writeInsert fd, page, index, object.record, _

      # Append the position to the page.
      @splice page, page.length, 0, position

      # Stash the position and object.
      cache[position] = object

    # If we are not an empty page, then append a positions array.
    if page.positions.length
      # Cache the objects we've read from the existing page.
      for position, object of cache
        @cacheRecord page, position, object.record, object.key

      # Write out our positions.
      @writePositions fd, page, _

    # Close our file.
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
  # To find a record in the b&#x2011;tree, we first use a tree of branch pages
  # to find the leaf page that contains our record.
  #
  # A branch page contains the addresses of ***child pages***. This array of
  # page addresses is essentially an *array of children*.
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
  # b&#x2011;tree collation using a first record in the referenced leaf page for
  # ordering.  That is, the first record of the leaf page is used as the key
  # associated with a page address in a penultimate branch page.
  #
  # The non-leaf nodes of a b&#x2011;tree have the property that the number of node
  # children is one greater than the number of keys. We obtain this property by
  # treating the first child as the left child of the entire page, and excluding
  # its key from the search. We search the subsequent keys to find the first key
  # that is grater than or equal to the record sought. If we encounter a key
  # that is less than all the keys in the branch page, we know that the record
  # is contained in the leaf page child associated with the key before it. We
  # are able to perform this search using binary search in logarithmic time.
  #
  # By ignoring the key of the first leaf page, the penultimate branch page has
  # a number of children that is one greater than the number of keys.
  #
  # **TK**: Not really explaining that it's only the left most leaf page that
  # is a special case. Suppose that I'm trying to introduce the concept.
  #
  # Notice that, when we are inserting a record into a leaf page other than the
  # left leaf page, we add it to a leaf page whose key is equal to or greater
  # than the penultimate branch key, so that the first record does not change,
  # and therefore that penultimate branch key does not change. The exception is
  # the left most leaf page, which accepts all the records less than the first key,
  # and therefore may accept a record less than its current least record.
  #
  # An insertion can only insert a into the left most leaf page of a penultimate
  # branch page a record less than the least record of the leaf page.
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
  # The key is obtained by descending the sub&#x2011;tree referenced by the
  # child. We first visit the branch page referenced by the child. We then visit
  # left children recursively, visiting the left child of the child, and the
  # left child of any subsequently visited children, until we reach a leaf page.
  # The first record of that leaf page is the key to associate with the child
  # address in the address array of the interior branch page.
  #
  # It is the nature of the b&#x2011;tree that keys move up to the higher levels
  # of the tree as pages split, while preserving the collation order of the
  # keys. When a branch page splits, a key from the middle of the page is chosen
  # as a partition. The partition is used as the key for the right half of the
  # split page in the parent page.
  #
  # Our implementation does not store the keys, as you may have noticed, but
  # descends down to the leaf page to fetch the record to use as a key.
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
  # Our b&#x2011;tree always contains a root page. The b&#x2011;tree is never so
  # empty that the root page disappears. The root page always has the same
  # address.
  #
  # **TK**: move. Until the root branch page is split, it is both the root
  # branch page and a penultimate branch page.
  #
  # ### Keys and First Records
  #
  # We said that it is only possible for an insertion to insert a into the left
  # most child leaf page of a penultimate branch page a record less than the
  # least record. We can say about a tree rooted by an interior branch page,
  # that an insertion is only able to insert into the left most leaf page in the
  # *entire tree* a record less than the least record.
  #
  # **TK**: Confusing.
  #
  # Using our example tree with one root interior page, with two penultimate
  # branch page children, we cannot insert a record into the right penultimate
  # branch page that will displace the first record of its left most child
  # branch, because that first record is the key for the right penultimate
  # branch page. When we insert a record that is less than the key, the search
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
  # This maintains a property of the b&#x2011;tree that for every leaf page
  # except the left most leaf page, there exists a unique branch page key
  # derived from the first record of the page.
  #
  # As above, you can find the first record used to derive a key by visting the
  # child and going left. You can find the leaf page to the left of the leaf
  # page used to derive a page branch key, by visiting the child to the left of
  # the key and going right.
  #
  # **NOTE**: Literate programming has finally materialized with Docco and
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
  # Our in-memory branch page is also cached and added as a node an MRU list. We
  # must make sure that each page has only one in memory representation, because
  # the in memory page is used for locking.

  #
  createBranch: (address, override) ->
    page =
      address: address
      addresses: []
      balancers: 0
      cache: {}
      length: 0
      locks: [[]]
      penultimate: true
      right: 0
      size: 0
    extend page, override or {}

  # #### Branch Page JSON Size

  # The branch page JSON size is JSON string length of the address array, plus
  # the JSON string length of each cached key.
  #
  # ##### JSON Reference Array Size
  #
  # The `splice` method adds or remove references from the reference array using
  # the semantics similar to JavaScript's `Array.splice`. Similar, but not
  # identical. The replacement values are passed in as an array, not as
  # arguments at the end of a variable argument list.
  #
  # This wrapper for a basic array operation exists for the sake of the JSON
  # size adjustments, which would otherwise be scattered around the code. It is
  # the only place where the JSON string length of the reference array is
  # accounted for.

  #
  splice: (page, offset, length, insert) ->
    # Get the references, either page addresses or record positions.
    values = page.addresses or page.positions

    # We remove first, then append. We used the array returned by `splice` to
    # generate a JSON substring, whose length we remove form the JSON size of
    # the page. We also decrement the page length.
    if length
      removals = values.splice(offset, length)

      json = if values.length is 0
        "[#{removals.join(",")}]"
      else
        ",#{removals.join(",")}"

      @heft page, -json.length

      page.length -= length
    else
      removals = []

    # Insert references.
    if insert?
      # Convert a single argument into an array.
      insert = [ insert ] if not Array.isArray insert
      # First we generate a JSON substring from the insert array, whose length
      # we add to the JSON size of the page. We also increment the page length.
      if insert.length
        json = if values.length is 0
          "[#{insert.join(",")}]"
        else
          ",#{insert.join(",")}"

        @heft page, json.length

        page.length += insert.length

        values.splice.apply values, [ offset, 0 ].concat(insert)
    # Return the removed references.
    removals

  # ##### JSON Key Size

  # Add a key to the branch page cache and recalculate JSON size. Uncache any
  # existing key for the address.
  cacheKey: (page, address, key) ->
    @uncacheKey page, address
    @heft page, JSON.stringify(key).length
    page.cache[address] = key

  # Remove a key from the branch page cache if one exists for the address.
  # Deduct the JSON string length of the key from the JSON size.
  uncacheKey: (page, address) ->
    if page.cache[address]
      @heft page, -JSON.stringify(page.cache[address]).length
      delete page.cache[address]

  # We write the branch page to a file as a single JSON object on a single line.
  # We tuck the page properties into an object, and then serialize that object.
  # We do not store the branch page keys. They are looked up as needed as
  # described in the b&#x2011;tree overview above.
  #
  # We always write a page branch first to a replacement file, then move it
  # until place using `replace`.

  #
  writeBranch: (page, suffix, _) ->
    filename = @filename page.address, suffix
    record = [ page.right, page.addresses ]
    json = JSON.stringify(record)
    line = "#{json} #{@checksum(json)}"
    buffer = new Buffer(line.length + 1)
    buffer.write line
    buffer[line.length] = 0x0A
    fs.writeFile filename, buffer, "utf8", _

  # To read a branch page we read the entire page and evaluate it as JSON. We
  # did not store the branch page keys. They are looked up as needed as
  # described in the b&#x2011;tree overview above.

  #
  readBranch: (page, _) ->
    # Read addresses from JSON branch file.
    filename = @filename page.address
    [ right, addresses ]  = @_readLine fs.readFile filename, "utf8", _

    # Splice addresses into page.
    @splice page, 0, 0, addresses

    # Extend the existing page with the properties read from file.
    extend page, { right }

  # ### B-Tree Initialization
  #
  # After creating a `Strata` object, the client will either open the existing
  # database, or create a new database.
  #
  # #### Creation
  #
  # Creating a new database will not create the database directory. The database
  # directory must already exist. It must be empty. We don't want to surprise
  # the application developer by blithely obliterating an existing database.
  #
  # The initial database has a single root penultimate branch page with only a
  # left child and no keys. The left child is a single leaf page that is empty.
  #
  # Note that the address of the root branch page is `0` and the address of the
  # left most leaf page is `-1`. This will not change. Even as the b&#x2011;tree
  # is balanced with splits and mergers of leaf pages, the root branch page is
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
    root = @encache @createBranch @nextAddress++, penultimate: true
    leaf = @encache @createLeaf -(@nextAddress++)
    @splice root, 0, 0, leaf.address
    # Write the root branch.
    @writeBranch root, ".replace", _
    @rewriteLeaf leaf, ".replace", _
    @replace leaf, ".replace", _
    @replace root, ".replace", _

  # #### Opening
  #
  # Opening an existing database is a matter checking for any evidence of a hard
  # shutdown. You never know. There may be a banged up leaf page file, one who's
  # last append did not complete. We won't know that until we open it.
  #
  # **TODO**: Ah, no. Let's revisit. Here's a simple strategy. Open touches a
  # file.  Closing deletes the file. If we open and the file exists, then we
  # probably have to inspect every file that was modified after the
  # modification, adjusting for dst? No because we'll be using seconds since the
  # epoch. Only if the system time is changed do we have a problem.
  #
  # Thus, we have a reference point. Any file after needs to be inspected. We
  # load it, and our `readLeaf` function will check for bad JSON, finding it
  # very quickly.
  #
  # Now, we might have been in the middle of a split. The presence of `*.new`
  # files would indicate that. We can probably delete the split. Hmm..
  #
  # We can add more to the suffix. `*.new.0`, or `*.commit`, which is the last
  # replacement. If we have a case where there is a file named `*.commit` that
  # does not have a corresponding permanent file, then we have a case where the
  # permanent file has been deleted and not linked, but all the others have
  # been, since this operation will go last, so we complete it to go forward.
  #
  # Otherwise, we delete the `*.commit`. We delete all the replacements that are
  # not commits.
  #
  # We can always do a thorough rebuild of some kind.
  #
  # Probably need "r" to not create the crash file, in case we're reading from a
  # read only file system, or something.

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

  # We close after every write, so there are no open file handles.
  #
  # **TODO**: Need to actually purge cache and set sizes to zero.

  # &mdash;
  close: (_) ->

  # **TODO**: Close medic file.

  # ### Concurrency
  #
  # Although there is only a single thread in a Node.js process, the
  # b&#x2011;tree is still a concurrent data structure.
  #
  # When we navigate the tree from the root to search for a key in the tree, we
  # descend the b&x2011;. The act of descending the b&#x2011;tree is called a
  # ***descent***.
  #
  # A descent is a unit of work in our concurrency model. One descent of the
  # b&#x2011;tree can make progress while another descent of the tree is waiting
  # on evented I/O. Instead of thinking about concurrency in terms of threads
  # we talk about concurrent descents of the b&#x2011;tree.
  #
  # Descents of the b&#x2011;tree can become concurrent when descent encounters
  # a page that is not in memory. While it is waiting on evented I/O to load the
  # page files, the main thread of the process can make progress on another
  # request to search or alter the b&#x2011;tree, it can make process on another
  # descent.
  #
  # This concurrency keeps the CPU and I/O loaded.
  #
  # Note that the b&#x2011;tree must only be read and written by a single
  # Node.js process. You can't have two Node.js processes serving the same
  # database directory.
  #
  # ### Locking
  #
  # Every page has a read/write lock. Pages are obtained through the `IO.lock`
  # method, so that every time we obtain a page, it comes to us locked.
  #
  # Locking is internal and not exposed to the application developer. However,
  # the application developer must remember to close cursors, in order to
  # release the locks that cursors hold.
  #
  # #### Shared and Exclusive
  #
  # When we obtain a lock to read the page, we obtain a ***shared lock***. The
  # shared lock allows other descents to also obtain a lock on the page. With a
  # shared lock, you can read the page, knowing that it will not change.
  #
  # When we obtain a lock to write the page, we obtain an ***exclusive lock***.
  # The exclusive lock prevents all other descents from obtaining any sort of
  # lock on the page. No other page will be able to obtain neither a shared nor
  # an exclusive lock.
  #
  # #### Locking Sub-Trees
  #
  # When we descend the tree, moving from parent page to child page, we obtain
  # locks in descending order. This means that if we obtain an exclusive lock on
  # a page, no other descent will be able to travel from the parent of that
  # page, to the children of that page. It effectively blocks new descents from
  # entering the ***sub&#x2011;tree*** that is defined by the exclusively locked
  # page and all its children.
  #
  # #### Rationale for Locking
  #
  # Locking prevents race conditions where a descent that is waiting on an
  # evented I/O request returns to find that the structure of the b&#x2011;tree
  # has changed drastically. For example, while a descent was waiting for a leaf
  # page to load into memory, another descent might load the same page into
  # memory, merge the leaf page with its sibling, and delete it.
  #
  # #### Caching
  #
  # Locks prevent cache purges. When a cache purge is triggered, a page with
  # outstanding locks will not be purged from the cache.
  #
  # #### Lock Properties
  #
  # The locking mechanism is a writer preferred shared read, exclusive write
  # lock. If a descent holds an exclusive write lock, then all lock requests by
  # other descents are blocked. If one or more descents hold a shared read lock,
  # then any request for an exclusive write lock is blocked. Any request for a
  # shared read lock is granted, unless an exclusive write lock is queued.
  #
  # The locks are not re-entrant.

  # &mdash;
  lock: (address, exclusive, callback) ->
    # We must make sure that we have one and only one page object to represent
    # the page. The page object will maintain the lock queue for the page. It
    # won't do to have different descents consulting different lock queues.
    # There can be only one.
    #
    # The queue is implemented using an array of arrays. Shared locks are
    # grouped inside one of the arrays in the queue element. Exclusive locks are
    # queued alone as a single element in the array in the queue element.

    #
    if page = @cache[address]
      # Move the page to the head of the most-recently used list.
      @link @mru, @_unlink page
    #
    else
      # Create a page to load with an empty `load` queue. The presence of the
      # `load` queue indicates that the page needs to be read from file.
      creator = "create#{if address < 0 then "Leaf" else "Branch"}"
      page = @encache @[creator](address, { load: [] })

    # #### Lock Implementation
    #
    # We don't use mutexes, of course, because Node.js doesn't have the concept
    # of mutexes to protect critical sections of code the way that threaded
    # programming platforms do.
    #
    # Nor do we use file system locking.
    #
    # Instead, we simulate locks using callbacks. A call to `lock` is an evented
    # function call that provides a callback. If the `lock` method can grant the
    # lock request to the caller, the lock method will invoke the callback.
    #
    # If the `lock` method cannot grant the lock request, the `lock` method will
    # queue the callback into a queue of callbacks associated with the page.
    # When other descents release the locks that prevent the lock request, the
    # lock request callback is dequeued, and the callback invoked.

    # The callback is always added to the queue, even if it is not blocked and
    # will execute immediately. The array in the queue element acts as a lock
    # count.
    #
    # If the callback we add to the queue is added to the first queue element,
    # then it is executed immediately. The first queue element is the active
    # queue element. Otherwise, it will be executed when the queue elements
    # before it have completed.
    #
    # When an exclusive lock is queued, an empty array is appended to the queue.
    # Subsequent read lock callbacks are appended to the array in the last
    # element. This gives exclusive lock callbacks priority.

    locks = page.locks
    if exclusive
      throw new Error "already locked" unless locks.length % 2
      locks.push [ callback ]
      locks.push []
      if locks[0].length is 0
        locks.shift()
        @load page, callback
    else
      locks[locks.length - 1].push callback
      if locks.length is 1
        @load page, callback

  # #### Check JSON Size

  # Here's a temporary block of code that will assert that we're keeping an
  # accurate count of the heft of our pages. After a while, this code will be
  # removed, and we'll count on assertions in our unit tests to catch errors.

  # This only check that the JSON size is correct for the give page contents,
  # not for the entire b&#x2011;tree.
  @checkJSONSize: (page) ->
    size = 0
    if page.address < 0
      if page.positions.length
        size += JSON.stringify(page.positions).length
      for position, object of page.cache
        { record, key } = object
        if object.size isnt JSON.stringify({ record, key }).length
          throw new Error "sizes are wrong"
        size += object.size
    else
      if page.addresses.length
        size += JSON.stringify(page.addresses).length
      for position, object of page.cache
        size += JSON.stringify(object).length
    if size isnt page.size
      throw new Error "sizes are wrong"

  # ### Load

  # One or more descents may encounter the same unloaded page. Only one descent
  # should load it, the others should wait.
  #
  # In `load` we ensure that only the first descent will actually invoke the
  # load function for the page. If we are the first to encounter an unloaded
  # page, we push our callback onto the `load` queue of the page, and invoke the
  # correct read function for the page time. We provide a callback that will
  # invoke all the queued callbacks in the `load` queue to the read function.
  #
  # If we encounter an unloaded page, but there are already callbacks in the
  # queue, we know that the first descent through has invoked read, and that our
  # callback will be invoked if we will simply place it in the `load` queue and
  # do nothing more.

  # &mdash;
  load: (page, callback) ->
    # If the page is not loaded, load it.
    if page.load
      # Add our callback to the list of waiting callback.
      page.load.push callback
      # If we are the first one through, create a group callback function, then
      # pass it to the load function for the page type.
      if page.load.length is 1
        # Create a callback that will invoke all the callbacks queued to wait
        # for the page to load.
        loaded = (error) ->
          for callback in page.load
            do (callback) -> process.nextTick -> callback error, page
          # On error we reset the load list.
          if error
            page.load.length = 0
          # Otherwise, we delete the load list, because no load list means the
          # page is loaded.
          else
            IO.checkJSONSize page
            delete page.load
        # Invoke the correct read function for the page type.
        if page.address < 0
          @readLeaf page, loaded
        else
          @readBranch page, loaded
    # If the page is already loaded, we wave the descent on through.
    else
      #
      IO.checkJSONSize page
      callback null, page

  # #### Unlock

  # When we release a lock, we simply shift a callback off of the array in the
  # first element of the queue to decrement the lock count. We are only
  # interested in the count, so it doesn't matter if the callback shifted by the
  # descent is the one that it queued.

  #
  unlock: (page) ->
    # Note that it is not possible for this method to be called on an page that
    # has not already been loaded.
    IO.checkJSONSize page
    locks = page.locks
    locked = locks[0]
    locked.shift()
    if locked.length is 0 and locks.length isnt 1
      locks.shift()
      # Each callback is scheduled using next tick. If any callback waits on
      # I/O, then another one will resume. Concurrency.
      for callback in locks[0]
        do (callback) -> process.nextTick -> callback error, page

  # Read a record cache entry from the cache. Load the record and cache it of it
  # is not already cached.
  stash: (page, position, _) ->
    if not stash = page.cache[position]
      record = @readRecord page, position, _
      stash = @cacheRecord page, position, record
    stash


  # A note on the theoretical `null` key. If the collation order places `null`
  # before all other values, that's a good choice, because that means that it
  # will never be used for a branch key. If it is used as a branch key, the
  # branch will never be able to cache the key value, it will always have to
  # look it up, because its cache entry for the key will be `null`.
  #
  # But, don't use a `null` key. Create a pseudo-duplicate `null` instead.

  # Get the key for the record in the case of a leaf page, or the key of the
  # branch child page in the case of a branch page. Because this method operates
  # on both branch pages and leaf pages, our binary search operates on both
  # branch pages and leaf pages.
  key: (page, index, _) ->
    if page.address < 0
      key = @stash(page, page.positions[index], _).key
    else if not key = page.cache[page.addresses[index]]
      [ iter, iterIndex, stack ] = [ page, index, [] ]
      while iter.address >= 0
        iter = @lock iter.addresses[iterIndex], false, _
        iterIndex = 0
        stack.push iter
      key = @stash(iter, iter.positions[iterIndex], _).key
      @unlock iter for iter in stack
      @cacheKey page, page.addresses[index], key
    key

  # Binary search implemented, as always, by having a peek at [Algorithms in
  # C](http://www.informit.com/store/product.aspx?isbn=0201314525) by [Robert
  # Sedgewick](http://www.cs.princeton.edu/~rs/).
  #
  # We set `low` to `1` to exclude a deleted ghost first record in a leaf page,
  # or the least child page of a branch page.
  #
  # Index is bitwise compliment of the insert location if not found.
  find: (page, key, low, _) ->
    { comparator } = @
    high = page.length - 1
    while low <= high
      mid = low + ((high - low) >>> 1)
      compare = comparator key, @key(page, mid, _)
      if compare > 0
        low = mid + 1
      else if compare < 0
        high = mid - 1
      else
        return mid
    ~low

# ## Descent
#
# We use the term *descent* to describe b&#x2011;tree operations, because all
# b&#x2011;tree operations require a descent of the b&#x2011;tree, a traversal
# of the b&#x2011;tree starting from the root. Whenever we are search the tree,
# insert or delete records, or balance the tree with a page splits and merges,
# we first begin with a descent of the b&#x2011;tree, from the root, to find the
# page we want to act upon.
#
# #### Descent as Unit of Work
#
# We use the term descent to describe the both traversal of the b&#x2011;tree
# and the subsequent actions performed when when the desired page is found.
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
# to b&#x2011;tree pages, just as we would with a multi-threaded b&#x2011;tree
# implementation.  When we descend the b&#x2011;tree we need to make sure that
# we do not alter pages that another waiting descent needs to complete its
# descent when it awakes, nor read pages that a waiting descent had begun to
# alter before it had to wait.
#
# These are race conditions. We use the shared read/exclusive write locks
# described in the `IO` class above to guard against these race conditions.
#
# #### Classes of Descent
#
# When we descend to leaf pages of a search b&#x2011;tree to obtain records, we
# *search* the b&#x2011;tree. When we change the size of the b&#x2011;tree by
# adding or deleting records we *edit* the b&#x2011;tree. When we change the
# structure of the b&#x2011;tree by splitting or merging pages, we *balance* the
# b&#x2011;tree.
#
# We talk about search descents, edit descents, and balance descents we we
# describe the interaction of b&#x2011;tree operations.
#
# We use these terms in this document to save the chore of writing, and the
# confusion of reading; insert or delete, or split or merge. We also want to
# draw a distinction between changing the count of records stored in the
# b&#x2011;tree, *editing*, and changing the height of the b&#x2011;tree, the
# count of pages, or the choice of keys, *balancing*.
#
# #### Locking on Descent
#
# Because a search descent does not alter the structure of the b&#x2011;tree,
# multiple search descents can be performed concurrently, without interfering
# with each other.
#
# Descents that alter the b&#x2011;tree require exclusive access, but only to
# the pages they alter. A search descent can still make progress in the presence
# of an alteration decent, so long as the search does not visit the pages being
# altered.
#
# A search descent obtains shared locks on the pages that it visits.  An
# alteration descent obtains exclusive locks only on the pages that it needs to
# alter. The alteration descent will obtain shared locks on the pages that
# visits in search the pages that it wants to alter.
#
# #### Locking Hand Over Hand
#
# To allow progress in parallel, we lock only the pages we need to descend the
# tree, for only as long as we takes to determine which page to visit next.
# Metaphorically, we descend the tree locking hand-over-hand.
#
# We start from the root page. We lock the root page. We perform a binary search
# that compares our search key against the keys in the root page. We determine
# the correct child page to visit to continue our search. We lock the child
# page. We then release the lock on the parent page.
#
# We repeat the process of locking a page, searching it, locking a child, and
# then releasing the lock on the child's parent.
#
# We hold the lock on the parent page while we acquire the lock on the child
# page because we don't want another descent to alter the parent page,
# invaliding the direction of our descent.
#
# #### Lateral Traversal
#
# Both branch pages and leaf pages are singly linked to their right sibling. If
# you hold a lock on a page, you are allowed to obtain a lock on its right
# sibling. This left right ordering allows us to traverse a level of the
# b&#x2011;tree, which simplifies the implementation of record cursors and page
# merges.
#
# When we move from a page to its right sibling, we hold the lock on the left
# page until we've obtained the lock on the right sibling. The prevents another
# descent from relinking linking our page and invalidating our traversal.
#
# #### Deadlock Prevention and Traversal Direction
#
# To prevent deadlock, we always move form a parent node to a child node, or
# form a left sibling to a right sibling.
#
# **TK**: Chunky.
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
    @exclusive  = false
    @depth      = 0
    @first      = true
    @index      = 0
    @page       = { addresses: [ 0 ] }

  # We use `fork` to create a new descent using the position of the current
  # descent. The new descent will continue to descend the tree, but without
  # releasing the lock on the page held by the descent from which we forked.
  #
  # When we split and merge pages other than the root page, we need to hold
  # locks on pages at more than one level of the tree. Pages at the parent level
  # will have nodes inserted or deleted. Pages at the child level will be split
  # or merged. When we reach a parent, we use `fork` to create a new descent, so
  # we don't release our lock on the parent when we visit the child.
  #
  # When we merge and delete leaf pages, we also need to update the key, which
  # may be at any level of the tree. We need to hold our lock on the branch page
  # that contains the key, while still descending the tree to find the two pages
  # that need to be merged. The two pages to merge may not be immediate children
  # of the same penultimate branch page.
  #
  # **TK** Glurgh: We need to leave the key page locked, then go left and right
  # to find two separate pages. We do not need to hold locks on all the pages
  # down to the targets, just the pivot point and the targets  The
  # hand-over-hand logic works fine. Descending hand-over-hand exclusive will
  # cause us to wait for other descents to finish, squeezing the other descents
  # out.

  # We create a new `Descent` which creates a dummy first page. We then assign
  # the addresses current descent to the dummy page, and copy the current index.
  fork: ->
    extend (new Descent @io), { @page, @exclusive, @index }

  @key: (key) ->
    (_) -> @io.find @page, key, (if @page.address < 0 then @page.ghosts else 1), _

  @found: (key)-> ->
    @page.addresses[0] != 0 && @io.comparator(@page.cache[@page.addresses[@index]],  key) == 0

  @leftMost: (_) -> @page.ghosts or 0

  # Stop when we reach a penultimate branch page.
  @penultimate: -> @page.addresses[0] < 0

  # Stop when we reach a leaf page.
  @leaf: -> @page.address < 0

  @discard: (_) -> 0

  # Follow the right most path. **TODO**: How much confusion do I save if I
  # replace addresses and positions with references?
  @right: (_) -> (@page.addresses or @page.positions).length - 1

  # All subsequent locks acquired by the descent are exclusive.
  exclude: -> @exclusive = true

  # Stop when we reach a certain depth in the tree.
  @depth: (depth) -> -> @depth is depth

  # Stop before a we descend to a child with a certain address.
  @address: (address) -> -> @address is address

  # Upgrade a lock from shared to exclusive. Works only with branch pages. All
  # subsequent locks acquired by the descent are exclusive.
  upgrade: (_) ->
    @io.unlock @page
    @page = @io.lock @page.address, @exclusive = true, _

  descend: (next, stop, _) ->
    while not stop.call(@)
      @depth++
      parent = @page
      @page = @io.lock parent.addresses[@index], @exclusive, _
      @io.unlock parent if @unlock
      @index = next.call(@, _)
      @unlock = true
      if @page.address >= 0 and @index < 0
        @index = (~@index) - 1

# ## Cursors
#
# Application developers navigate the b&#x2011;tree using one of two types of
# ***cursor***. To read records in the b&x2011;tree they use an ***iterator***.
# To read records, as well as insert and delete records, they use ***mutator***.
#
# An iterator provides random access to the records in a page. It can move from
# a page to the right sibling of the page. A mutator does the same, but it is
# also able to insert or delete records into the current page.
#
# ### Iterator
#
# The application developer uses an iterator to move across the leaf pages of
# the b&#x2011;tree in ascending collation order, one leaf page a time.
#
# #### Search Keys
#
# The application developer obtains an iterator by calling `Strata.iterator`
# with a ***search key***.  The search key is used to find the leaf page where
# the record from which the key is derived belongs in the b&#x2011;tree. The
# record may not actually exist in the b&#x2011;tree, in which case the iterator
# begins with the leaf page where record *would* be.
#
# #### Page by Page
#
# The leaf pages themselves are visited one at a time, not the records. The
# iterator can randomly access any record in the currently visited leaf page.
#
# When a page is visited it is read locked, so that other descents can visit the
# page, but they cannot insert or delete records. By locking the pages left to
# right hand over hand, then there is no way for the tree to mutate such that
# would defeat our iteration. Leaf pages that we've visited may by edited by
# another descent after we've visited them, however.
#
# **TK**: Definition of keys and records. Go back up and make sure one is there.
#
# #### Record Ranges
#
# The cursor will define an `offset` property and a `length` property. The
# `offset` is positioned at the first record in the page whose key is equal to
# or greater than the search key. The `length` is the count of records in the
# page. This defines the range of records whose key is greater than or equal to
# the search key.
#
# On the first page visited, the key of the record at the `index` is greater
# than or equal to the search key. Every key of every record that follows the
# record at the index is greater than the search key.
#
# #### Full Tree Iteration
#
# The application developer can also obtain an iterator that begins at the left
# most leaf page by calling `Strata.iterator` without a search key. This
# positions the iterator at the first leaf page in the b&#x2011;tree and the
# index at the first record in b&#x2011;tree.
#
# #### Double Loop
#
# The interface to iteration requires the application developer to implement
# double loop to traverse the leaf pages. The outer loop moves from page to
# page. The inner loop moves from record to record. The iterator interface does
# not hide the underlying structure of the leaf pages.
#
# It is not intended to be an abstraction. It is intended to expose the
# structure. Do not confuse the iterator with an iterator from other APIs that
# exposes one item at a time. Our iterator exposes a range of records.
#
# #### Ranged Searches
#
# Ranged searches are performed by searching for the start of the range and
# iterating to the end of the range. There is nothing to this. It is how
# iterator is implemented.
#
# We may be interested in searching for time series data that occurred between
# noon and midnight, where are time stamp is POSIX time, milliseconds since the
# epoch.  We create an iterator with a search key that is noon according to
# POSIX time. It doesn't matter to us if there were no events that occurred
# exactly at the millisecond that defines noon. Our iterator begins at the point
# that is either an event that occurred millisecond that defines noon, or else
# the first event that occurred after the noon millisecond.
#
# When we encounter to the first event that occurs after midnight, we ignore
# that event and terminate traversal. We've successfully found all the events in
# our range.

#
class Iterator

  # Iterators are initialized with the results of a descent.
  constructor: (@key, { @index, io, page, exclusive }) ->
    @_io = io
    @_page = page
    @exclusive = exclusive
    @length = @_page.positions.length
    @offset = if @index < 0 then ~ @index else @index

  # Get a the record at a given index from the current leaf page.
  get: (index, _) ->
    @_io.stash(@_page, @_page.positions[index], _).record

  # Go to the next leaf page, the right sibling leaf page. Returns true if there
  # is a right sibling leaf page of the current page, false if there the current
  # leaf page is the last leaf page in the b&#x2011;tree.
  next: (_) ->
    # If we are not the last leaf page, advance and return true.
    if @_page.right
      # If we are iterating for insert and delete, we may already have taken a
      # peek at the next page.
      if @_next
        next = @_next
        @_next = null
      # Otherwise fetch the next page.
      else
        next = @_io.lock @_page.right, @exclusive, _

      # Unlock the current page.
      @_io.unlock @_page

      # Advance to the next page.
      @_page = next

      # Adjust the range.
      @offset = @_page.ghosts
      @length = @_page.positions.length

      # We have advanced.
      true

  # Get the index of the record from which the given key is derived, or else the
  # bitwise compliment of index where record would be inserted if no such record
  # exists in the leaf page.
  indexOf: (key, _) ->
    @_io.find @_page, key, @_page.ghosts, _

  # Unlock all leaf pages held by the iterator.
  unlock: ->
    @_io.unlock @_page
    @_io.unlock @_next if @_next

# ### Mutator

# A mutator is an iterator that can also edit leaf pages. It can delete records
# from the currently visit leaf page. It can insert records into the current
# leaf page, if the record belongs in the current leaf page.
#
# As with `Iterator`, it moves across the leaf pages of the b&#x2011;tree in
# ascending collation order. It has random access to the records in the page
# using an index into the array of records.
#
# As with `Iterator`, the application developer obtains an iterator by calling
# `Strata.mutator` with a search key.
#
# #### Ranged Inserts
#
# You can insert a range of records using a single mutator. This is efficient if
# you have a range of records whose keys are close together, maybe so close that
# they are all on the same page, so you can descend the tree to the correct page
# and insert them in one fell swoop.
#
# #### Ambiguous Insert Locations
#
# Insert locations for ranged inserts can be ambiguous if the binary search
# indicates that a record should be inserted at the end of the leaf page, after
# the current last record in the leaf page. If the insert location is after the
# last record, it could be the case that the record really belongs to a right
# sibling leaf page.
#
# This is only a problem when we insert a record whose key is not the key used
# to create the mutator. An insert location is always unambiguous if the key is
# the search key used to locate the first page. The key is determined to belong
# inside the leaf page by virtue of a descent of the b&#x2011;tree. That is
# unambiguous.
#
# To know if a subsequent record insert really does belong after the last record
# but before the first record of the right sibling leaf page, we have to load
# the right sibling leaf page and peek at the record. When we do this, we need
# to keep the right sibling leaf page locked, so that the key of the right
# sibling page cannot change.
#
# This peek has a cost. If you are inserting a range, and the records are more
# often pages apart from each other than they are on the same page, it might not
# be worth it to peek. It might be more efficient to assume that the next record
# is much further along and create a new mutator for the remainder of the insert
# range. In this case we're saying, see if you can insert the next record on
# this page as long as we're here, but we leaf traversal is inefficient for our
# range,  so don't try too hard.
#
# If she is only inserting a single record, there's no ambiguity, because she'll
# use the key of the record to create the mutator. There is no need to enable
# peek for a single insert, but there is no real cost either.
#
# #### Duplicate Keys
#
# Although duplicate keys are not allowed, abstracted duplicate keys are not
# difficult for the application developer to implement given a mutator. The
# application developer can move forward through a series and append a record
# that has one greater than the maximum record.  Not a problem to worry about
# ambiguity in this case. Ah, we need to peek though, because we need to get
# that number.
#
# **TK**: Fix.
#
# In fact, given a key plus a maximum series value, you will always land after
# the last one the series, or else a record that is less than the key, which
# means that the series is zero. Deleted keys present a problem, so we need to
# expose a leaf page key to the user, which, in case of a delete, is definitely
# the greatest in the series.
#
# **TODO**: Zero is a valid index for the left most leaf page.

#
class Mutator extends Iterator
  # If the insert index of the record is after the last record, and upon peeking
  # at the first record of the right sibling leaf page we determine that the
  # record belongs on a subsequent page, `insert` return `null`.
  #
  # An exception is raised if the record belongs in a page that is a left
  # sibling leaf page of the current leaf page.

  #  &mdash;

  # Insert the record with the given key at the given index. Obtain the correct
  # index to preserve the sort order by calling `indexOf`.
  #
  # If the index is after the last record in the leaf page, this method will
  # check that the record does not actually belong in a subsequent sibling leaf
  # page.
  #
  # If there is a right sibling leaf page, it will load the right sibling leaf
  # page and check that the leaf is less than the key of the right sibling leaf
  # page. If the key of the insert record is greater than the key of the right
  # sibling leaf page, then the record does not belong in this leaf page. The
  # record will not be inserted. The method returns `false`.
  #
  # This method will happily accept all other forms of invalid data. The
  # application developer is responsible for maintaining the collation order of
  # the leaf page. The application developer must not insert duplicates. The
  # application developer must make sure to provide a `record`, `key` and
  # `index` that correspond to each other. No assertions are performed on the
  # validity of the insert.
  #
  # #### Avoiding the Peek
  #
  # There is a cost involved with peeking at the right sibling leaf page to
  # determine if a record greater than the greatest record in the current leaf
  # page belongs in the current page, or in a subsequent leaf page. If the
  # application developer doesn't want to peek, they can take matters into their
  # own hands. They can determine the insert location using `indexOf`, and if it
  # is after the last record, they can use a new mutator to find the insert
  # location of the next page.
  #
  # There is no cost involved when inserting a range into the last leaf page, a
  # common operation, because the right sibling leaf page does not exist, so
  # there is no doubt that the records belong on the last page.
  #

  #
  insert: (record, key, index, _) ->
    # On every leaf page except the first leaf page, the least record is the
    # key, and inserted records are always greater than the key. We assert this
    # here. Do not catch this exception, debug your code.
    if index is 0 and @_page.address isnt -1
      throw new Error "lesser key"

    # An insert location is ambiguous if it would append the record to the
    # current leaf page.
    unambiguous = index < @_page.length

    # If we are at the first leaf page and the key is the search key that got us
    # here, then this is, without a doubt, the correct leaf page for the record.
    unambiguous or= @key and @_io.comparator(@key, key) is 0

    # An insert location is unambiguous if  we are the last page. There is no
    # subsequent page to which the record could belong.
    unambiguous or= not @_page.right

    # An insert location is ambiguous if we have an ambiguous insert location,
    # peek at the next leaf page to see if the record doesn't really belong to a
    # subsequent leaf page.
    if not unambiguous
      # The lock must held because the balancer can swoop in and prune the ghost
      # first records and thereby change the key. It could not delete the page
      # nor merge the page, but it can prune dead first records.

      #
      @_next or= @_io.lock @_page.right, @exclusive, _
      unambiguous = @_io.comparator(key, @_io.key(@_next, 0, _)) < 0

    # If insert location is unambiguous, insert the record and return the insert
    # index, otherwise return `undefined`.
    if unambiguous
      # Cache the current page length.
      @_io.balancer.unbalanced(@_page)

      # Since we need to fsync anyway, we open the file and close the file when
      # we append a JSON object to it. Because no file handles are kept open,
      # the b&#x2011;tree object can left to garbage collection.
      filename = @_io.filename @_page.address
      fd = fs.open filename, "a", 0o644, _
      position = @_io.writeInsert fd, @_page, index, record, _
      fs.close fd, _

      # Insert the position into the page a cache the record.
      @_io.splice @_page, index, 0, position
      @_io.cacheRecord @_page, position, record, key

      # Update the length of the current page.
      @length = @_page.positions.length

    # Return true if we inserted the record.
    unambiguous

  # Delete the record at the given index. The application developer is
  # responsible for providing a valid index, in the range defined by the
  # `offset` and `length` of the cursor, or else the `ghosts` and `length` of
  # the `page`.
  delete: (index, _) ->
    # Record the page as unbalanced.
    @_io.balancer.unbalanced(@_page)

    # If we're deleting the leaf page key, we ghost the key.
    ghost = @_page.address isnt -1 and index is 0

    # Append a delete object to the leaf page file.
    filename = @_io.filename @_page.address
    fd = fs.open filename, "a", 0o644, _
    position = @_io.writeDelete fd, @_page, index, _
    fs.close fd, _

    # If we've created a ghost record, we don't delete the record, we simply
    # move the `ghosts` for the page forward to `1`. If the current offset of
    # the cursor is `0`, we move that forward to `1`. Otherwise, we uncache and
    # splice the record.
    if ghost
      @_page.ghosts++
      @offset or @offset++
    else
      @_io.uncacheRecord @_page, @_page.positions[index]
      @_io.splice @_page, index, 1
      @length = @_page.length

# #### Insertion and Deletion Versus Balance
#
# We do not attempt to balance the tree with every insertion or deletion. The
# client may obtain a cursor to the leaf pages, iterate through them deleting
# records along the way. As the client alters leaf records, they are marked as
# candidates for balance. Balancing will take place periodically, where a single
# thread of control **TK** will
#
# #### Staccato Balance Operations
#
# The b&#x2011;tree balance operations cascade by nature. If you insert a value
# into a leaf node, such that the leaf node is beyond capacity, you split the
# leaf node, adding a new child to the parent node. If the parent node is now
# beyond capacity, you split the parent node, adding a new child to its parent
# node. When every node on the path to the leaf node is at capacity, a split of
# the leaf node will split every node all they way up to the root.
#
# Merges too move from leaves to root, so that a merge at one level of the
# b&#x2011;tree potentially triggers a merge of the parent with one of its
# siblings.
#
# However, we've established rules for lock acquisition that require that locks
# are obtained from the top down, and never from the bottom up. This is why we
# do not perform balance operations as a part of a single pass. We instead
# descend the tree once to insert or delete records form the leaf pages. We then
# descend the tree once for each split or merge of a page.
#
# Much b&#x2011;tree literature makes mention of a potential efficiency where
# you split full pages on the way back up from an insert. You can determine
# which pages would split if the leaf split as you descend the b&#x2011;tree,
# since you'll visit every page that would participate in a split.
#
# That efficiency applies only for split, and not for merge, because you have to
# inspect the left and right siblings of a page to determine if it is time to
# merge. If the left sibling page of a page, is not also child of that page's
# parent page, then the left sibling page is in a different sub&#x2011;tree. It
# can not be reached by the path that was used to find the leaf page where the
# delete occurred.
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
# If out balance operation cascades so that it requires a balance at every
# level, we'll descend the tree once for every level. However, the path we
# follow is almost certain to be in memory, since we're revisiting the same
# path.
#
# Also, a balance operation will involve an increasing number of levels with
# decreasing frequency. A split will most often require that only the leaf page
# is split. The penultimate pages will be involved in a balance operation at
# least an order of magnitude less frequently. The pages above the penultimate
# branch pages will be involved in a balance operation yet another order of
# magnitude less frequently.
#
# Conserving descents during balance operations is a false economy. It
# complicates lock acquisition. It reduces the liveliness of the b&#x2011;tree.
#
# The multiple descents will allow searches of the b&#x2011;tree to make
# progress between balance operations.
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
# Because the b&#x2011;tree is a concurrent structure, the leaf split descent
# may discover that another descent has removed a record, and a leaf split is no
# longer necessary. There may be, in fact, a descent on the way to the left
# sibling of the page, to check for the potential for a merge.
#
# The concurrent operation means that we have to deal with situation where we've
# undertaken a descent to balance the b&#x2011;tree, but another series of
# descents has rendered that plan invalid.
#
# As long as we're dealing with that problem, we may as well decouple insertion
# and deletion form split and merge entirely, and see if we can't gain more
# liveliness, and a simpler implementation, by separating these concerns.
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
# our b&#x2011;tree, however, multiple descents can make progress at the time,
# or rather, the progress made by one decent, while another descent waits on
# I/O.
#
# We ensure that only one descent at a time is making progress toward the
# balance of the tree. This simplifies or balance implementation, because the
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
# b&#x2011;tree. When an insert is performed, the offset count for the leaf page
# is incremented.  When a delete is performed, the offset count for the leaf
# page is decremented. This keeps track of the total change in size.
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
# loaded by the balancer. We also set the balancer flag, indicating that we need
# to preserve the page for record count, even if the actual page data is
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
# Splitting is the simpler of the two balancing operations.
#
# To split a leaf page, we start by obtaining the key value of the first record.
# We can do this by acquiring a read lock on the page, without performing a
# descent. The balancer gets to break some rules since it knows that we know
# that the b&#x2011;tree is not being otherwise balanced.
#
# We descend the tree until we encounter the penultimate branch page that is the
# parent of the branch page.  We acquire an exclusive lock the branch page. We
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
# deplete, the branch pages cannot be allowed to become empty as we merge leaf
# pages. As we delete records, we still keep a ghost of a key. As we delete leaf
# pages, we delete the ghost keys. Branch pages become paths to nowhere. They
# don't hold their own keys, so they can't find them.  We'd have to have null
# keys in our tree. Even if we kept keys around, we're sending searches down a
# path to nowhere. There is no leaf page to visit. We get rid of these paths. We
# always balance the upper levels immediately, we perform the cascade. Our tree
# descent logic would have to account for these empty sub&#x2011;trees. Much
# better to balance and keep things orderly.
#
# This raises a concerns about live lock, that we might be balancing
#
# **TK**: Yes file times are enough. Even if the system clock changes
# drastically, the file times are all relative to one another. It it changes
# during operation, that is a problem, but we're not going to endeavor to
# provide a solution that deals with erratic clock times. Worst case, how do we
# not detect a file in need of recovery? We ignore files older than the time
# stamp file. So, we might have the system clock move far backward, so that the
# time stamp file is much newer than all the files that are being updated. Oh,
# well. What's the big deal then? How do we fix that? If it is a server install,
# we demand that you maintain your clock. If it is a desktop install, we can
# comb the entire database, because how big is it going to be?
#
# Hmm... What are you going to do? This is why people like servers.
#
# ### Merging
#
# **TODO**: Great example floating around. Imagine that you've implemented MVCC.
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
# When we encounter the key value in a branch page, we acquire an exclusive lock
# the branch page. We can release our shared lock and acquire an exclusive lock.
# We do not have retest conditions after the upgrade, because only the balancer
# would change the keys in a branch page, and we're the balancer.
#
# We then descend the child to the left of the key, instead of to the right as
# we would ordinary. We descend to the left child acquiring, an exclusive lock,
# but retaining our exclusive lock on branch page where we found our key. We
# then descend to the right most child of every child page, acquiring exclusive
# locks in the hand-over-hand fashion, until we reach a leaf page. We are now at
# the left sibling of the page we want to merge.
#
# We've locked the branch page that contains the key exclusively so that we can
# reassign the key. It will no longer be valid when the page is merged into its
# left sibling because the first record is now somewhere in the midst of the
# left sibling. We lock exclusively hand-over-hand thereafter to squeeze out any
# shared locks. Our exclusive lock on the parent containing the key prevents
# another descent from entering the sub&#x2011;tree where we are performing the
# merge.
#
# We now proceed down the path to the merge page as we would ordinarily, except
# that we acquire exclusive locks hand-over-hand instead of shared locks. This
# will squeeze out any other descents.
#
# We retain the exclusive lock on the penultimate branch page. No other descent
# will be able to visit this penultimate branch, because we've blocked entry
# into the sub&#x2011;tree and squeeze out the other descents. We still need to
# hold onto the exclusive lock, however, otherwise the page might be discarded
# during a cache purge, which can happen concurrently.
#
# We append the records in the merge page to its left sibling. We remove the
# address of the merged page from the penultimate page.
#
# If we've deleted the first child of the penultimate branch page, then we
# delete the cached key of the new first child. The new first child is the
# left-most child of the penultimate page. Its key, if it not the left-most page
# of the entire tree, has been elevated to the exclusively locked branch page
# where we encountered the merge page key. We don't want keys to gather where
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
# We acquire the key for the left most page in the sub&#x2011;tree underneath
# the branch page. We do this by following the left most children until we reach
# a leaf page. We use that key to descend the tree.
#
# we lock the page exclusively. We retain that lock for the duration of the
# merge.
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
# Before we lose track of the sub&#x2011;tree we're in, we descend to the
# potentially new left most leaf of the parent, and obtain its key to repeat the
# process.
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
# we will have a consistent depth. This is merge.
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
# while. It is up the end user.
#
# The end user can use the b&#x2011;tree a map, tucking in values, getting them
# out. Or else, as an index, to scan, perform table scans. We'll figure that
# out.
#
# Now I have an API problem. The client will have to know about pages to work
# with them. We can iterate through them, in a table scan. We can implement a
# merge. We probably need an intelligent cursor, or a reporting cursor.

# There has been a desire second guess the most-recently used list. There is a
# desire to link nodes to the back of the list or suspend purges.
#
# There is a desire to cache the addresses of the left sibling pages when
# possible, so I wouldn't have to descend the tree. Except that the
# most-recently used list works hand in hand with b&#x2011;tree descent. The
# higher levels of the tree are kept in memory, because they are more frequently
# visited than lower levels. To much iteration along one level threatens to
# purge other levels.
#
# One can imagine that when balancing b&#x2011;tree that has been left
# unbalanced for a long time, reading in many unbalanced leaf pages will cause
# the first ones to flush, which is a pity, since we'll probably need one of
# them.
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
# defer to the algorithms. The simpler the code, the more trust you can have in
# the code, the more likely your code will be adopted. A wide user base can
# inform decisions on optimization. There is always a core of what your
# application needs to do, and Strata needs to search and edit records.
#
# Balancing the tree is maintenance. The balancer can take its time.

class Balancer
  constructor: (@leafSize) ->
    @lengths = {}
    @operations = []
    @referenced = {}

  # Mark a page as having been altered, now requiring a test for balance. If the
  # `force` flag is set, the value is set to the leaf order, so that if the
  # record count of the page is less than the order of the leaf page, it will be
  # test for merge. If it is greater than the order of the leaf page, it will be
  # split. Of course, if it the order of the page, it can not be merged, nor
  # should it be split.
  unbalanced: (page, force) ->
    if force
      @lengths[page.address] = @leafSize
    else
      @lengths[page.address]?= page.length - page.ghosts

  # TODO If it is not exposed to the user, I don't underbar it.
  reference: (page) ->
    if not @referenced[page.address]
      @referenced[page.address] = page
      page.balancers++

  # **TODO**: You will have to launch this in a worker thread, otherwise there
  # is no good way for you to handle the error, or rather, you're going to have
  # to have some form of error callback, which is a pattern I'd like to avoid.

  # **TODO**: Uh, no. You can kind of launch this whenever the who, so long as
  # you do not launch more than one at a time. Use a callback. The callback can
  # record your errors to an error log. Do note that balance is always
  # concurrent, though. Makes no sense to try to run more than one at a time, or
  # it doesn't make sense to run a balance when a balance is running, or rather,
  # it doesn't make sense to make it a matter of running one after each insert.
  # Horrible writing here. Do not use.

  # **TODO**: Once loaded, and marked as part of the balancer, we can do our
  # calculations in one fell swoop. This triggers the consternation over what
  # all these extraneous pages do to the cache.

  # **TODO**: What is the procedure for vacuuming deleted keys? &mdash; We check
  # every page that has been added to the balancer, regardless of whether it has
  # grown, shrunk or returned to its original reported size. If the page is to
  # be deleted, because the leaf page is empty, that negates any fussing with
  # the key. Same goes for the case where the page is to be merged.

  # Ah, also, when we do load these, when we want to get them from the cache, we
  # don't really need them to be loaded. We should reach in a probe the cache
  # ourselves. My former Java self would have to spend three days thinking about
  # encapsulation, probably create a new sub-project. Agony.

  # Balancing will continue until morale improves. It may feel like a race
  # condition, but that can't be helped. There may be degenerate use cases where
  # the b&#x2011;tree cannot reach a balanced state. Inserts and deletes may be
  # taking a set of pages from needing split to needing merge faster than the
  # balance plan can figure it, and the balance operations can make it so.

  # The balance plan is optimistic. It creates a plan based on the state of the
  # tree a specific point in time. While implementing the plan, however, the
  # state of the tree may change, invalidating aspects of the plan. In this
  # case, an operation will be canceled. When an operation is canceled, we add
  # the canceled pages to the next balancer.

  # **TODO**: Tracking the difference means we can short cut planning, if the
  # page has only grown. This is a short cut. We consider its use carefully. We
  # are not capricious with it. We are okay with having to load the sibling page
  # lengths to check for merge. A split will lead to a subsequent balance plan
  # that will load four pages. In that regard, splits are not cheaper than
  # merges.

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
    addresses = Object.keys @lengths
    return if addresses.length is 0

    ghosts = {}
    max = @io.options.leafSize

    # We put a new balancer in place of the current balancer. Any edits will be
    # considered by the next balancer.
    @io.balancer = new Balancer
    @io.balancer.balancing = true

    # Prior to calculating a balance plan, we gather the sizes of each leaf page
    # into memory. We can then make a balance plan based on page sizes that will
    # not change while we are considering them in our plan. However, page size
    # may change between gathering and planning, and page size may change
    # between planning and executing the plan. Staggering gathering, planning
    # and executing the balance gives us the ability to detect the changes in
    # page size. When we detect that we can't make an informed decision on a
    # page, we pass it onto the next balancer for consideration at the next
    # balance.

    # For each page that has changed we add it to a doubly linked list.
    ordered = {}
    for address in addresses
      hit "865730cf41adf339f4b1459c89a036b2"
      # Convert the address back to an integer.
      address = + address
      length = @lengths[address]

      # We create linked lists that contain the leaf pages we're considering in
      # our balance plan. This is apart from the most-recently used list that
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
        hit "9542e06ace5cf0348025669c959605c3"
        page = @io.lock address, false, _
        node = { page, length: page.length - page.ghosts, key: @io.key(page, 0, _)  }
        @reference page
        @io.unlock page
        ordered[page.address] = node

      if node.page.ghosts
        hit "94b9d95f933238d1c84ae03309882549"
        ghosts[node.page.address] = node

      # If the page has shrunk in size, we gather the size of the left sibling
      # page and the right sibling page. The right sibling page
      if node.length - length < 0
        hit "f349ecda729348fd6584808d362ccbe0"
        if node.page.address isnt -1
          hit "d8016f324e06790f7946568f0587947d"
          if not node.left
            hit "7e6857af475a750affb42d11df04b825"
            descent = new Descent(@io)
            descent.descend Descent.key(node.key), Descent.found(node.key), _
            # **TODO**: You know that this would drive you mad and cost you 3
            # days if you were a Java programmer. Encapsulation! Encapsulation!
            descent.index--
            descent.descend Descent.right, Descent.leaf, _
            # Check to make sure we don't already have a node for the page.
            left = { page: descent.page, length: descent.page.length - descent.page.ghosts, key: @io.key(descent.page, 0, _) }
            @reference left.page
            @io.unlock left.page

            ordered[left.page.address] = left

            left.right = node
            node.left = left
        if not node.right and node.page.right
          hit "3175111e9f52d1ca2ee17752a303d847"
          if not right = ordered[page.right]
            hit "29dcf1e23e38e591067b26ecac75fbb3"
            page = @io.lock page.right, false, _
            ordered[node.page.right] = right = { page, length: page.length - page.ghosts, key: @io.key(page, 0, _) }
            @reference page
            @io.unlock page
          node.right = right
          right.left = node

      # Save us the trouble of possibly going left for a future count, if we
      # have an opportunity to make that link from the right free of a descent.
      else if not node.right and right = ordered[page.right]
        hit "79538567b69d7f9cd75393cf9eaa90c7"
        node.right = right
        right.left = node

    # The remainder of the calculations will not be interrupted by evented I/O.
    # Gather the current counts of each page into the node itself, so we adjust
    # the count based on the merges we schedule.
    for address, node of ordered
      hit "c7c40fcf0412d8ef67d6aebf13bf4578"
      node.length = node.page.length - node.page.ghosts

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
    # while we were waiting evented I/O. Note how we drop the page if a split is
    # not necessary.

    #
    for address, length of @lengths
      hit "c3e49bad2a714b71d8f02ba6a5d0c1ed"
      node = ordered[address]
      difference = node.length - length
      # If we've grown past capacity, split the leaf. Remove this page from its
      # list, because we know it cannot merge with its neighbors.
      if difference > 0 and node.length > max
        hit "f7c05e6daecc9482b0a56033bf82979b"
        # Schedule the split.
        @operations.push method: "splitLeaf", key: node.key
        # Unlink this split node, so that we don't consider it when merging.
        unlink node.left
        unlink node
      # Lost a race condition. When we fetched pages, this page didn't need to
      # be tested for merge, so we didn't grab its siblings, but it does now.
      # We ask the next balancer to consider it as we found it.
      else if difference < 0 and not ((node.page.address is -1 or node.left) and (node.page.right is 0 or node.right))
        hit "00472eae15631d485559e87d85475060"
        @io.balancer.lengths[node.page.address] = length

    # Now remove any node from our ordered collection that is not the left most,
    # so that we have a collection of heads of linked pages.
    for address in Object.keys ordered
      hit "6357544596e24b3bf1500768c7d28e7a"
      delete ordered[address] if ordered[address].left

    # We schedule merges, removing the nodes we merge and the nodes we can't
    # merge until the list of nodes to consider is empty.
    loop
      # We're done where there are no more nodes to consider.
      addresses = Object.keys ordered
      break if addresses.length is 0
      hit "a99a8d4110b74e78e38872f61460f3f6"

      # Break the links between pages that cannot merge.
      for address in addresses
        hit "d24e07a6f1b65a4d7cddb734d8fc742b"
        node = ordered[address]
        while node.right
          hit "b71fce84e1cbc61be53656f7a54aa1b7"
          if node.length + node.right.length > max
            hit "d5c4684a7e3501d218f3a4e17dcd00cc"
            node = unlink node
            ordered[node.address] = node
          else
            hit "cc831aedf992c3ed86c7f521a01bc4a7"
            node = node.right

      # Merge the node to the right of each head node into the head node.
      for address in addresses
        hit "5e49179d14c5d9c8e985576606544b36"
        node = ordered[address]
        # Schedule the merge. After we schedule the merge, we increase the size
        # of the head node and link the head node to the right sibling of the
        # right node. Note that a leaf page merged into its left sibling will
        # be destoryed, so we don't have to tidy up its ghosts.
        if node.right
          hit "585b636910747266f2e8c8e05bbf83fa"
          right = unlink node
          delete ghosts[right.page.address]
          @operations.push
            method: "mergeLeaves"
            key: right.key
            unbalanced: @lengths
          node.length += right.length
          link node, unlink right
        # Remove any lists containing only one node.
        else
          delete ordered[address]

    # Rewrite position arrays to remove ghosts.
    for address, node of ghosts
      @operations.unshift
        method: "deleteGhost"
        key: node.key

    # Perform the operations to balance the b&#x2011;tree.
    for operation in @operations
      @[operation.method].call(@, operation, _)

    # Decrement the reference lengths. **TODO**: Why a length and not a boolean?
    for address, page of @referenced
      page.balancers--

  # ### Should We Split a Branch?
  #
  # Thank goodness for Streamline.js. We can recursively call split to split our
  # branch pages, if they need to be split.
  #
  # We call this method with unlocked branch pages. That's okay, because only
  # the balance can alter a branch page. Even if the unlocked branch page is
  # purged from the cache, and subsequently reloaded, the address and length of
  # the page it represents will not change.

  # &mdash;
  shouldSplitBranch: (branch, key, _) ->
    # Are we larger than a branch page ought to be?
    if branch.length > @io.options.branchSize
      # Wait a tick!
      process.nextTick _
      # Either drain the root, or split the branch.
      if branch.address is 0
        @drainRoot _
      else
        @splitBranch branch.address, key, _

  # **TODO**: What if the leaf has a deleted key record? Abandon. We need to
  # have purged deleted key records before we get here. For example, it may be
  # the case that a leaf page key has been deleted, requiring a page key swap.
  # The page might also need to be split. We push the split onto the next
  # balance. (Bad example, we really don't have to do this.)
  #
  # **TODO**: When added to the balancer, we note the size of the leaf page when
  # it was last known to be balanced in relation to its siblings. Until we can
  # either delete it or run it through a plan where it is known to be balanced,
  # it is in an unbalanced state.
  #
  # **TK**: Docco.

  #
  splitLeaf: ({ key }, _) ->
    # Keep track of our descents so we can unlock the pages at exit.
    descents = []

    # We descend the tree directory directly to the leaf using the key.
    descents.push penultimate = new Descent(@io)
    sought = Descent.key(key)

    # Descend to the penultimate branch page, from which a leaf page child will
    # be removed.
    penultimate.descend(sought, Descent.penultimate, _)
    penultimate.upgrade(_)

    # Now descend to our leaf to split.
    descents.push leaf = penultimate.fork()
    leaf.descend(sought, Descent.leaf, _)
    split = leaf.page

    # If it turns out that our leaf has drained to the point where it does not
    # need to be split, we should then check to see if it can be merged.

    # **TODO**: We're not bothering with split when we've only grown a bit,
    # right?
    if split.length <= @io.options.leafSize
      @_io.balancer.unbalanced(@_page)

    # Otherwise we perform our split.
    else
      # It may have been some time since we've split, so we might have to split
      # into more than two pages.
      pages = Math.ceil(split.length / @io.options.leafSize)
      records = Math.floor(split.length / pages)
      remainder = split.length % pages

      right = split.right

      replacements = []
      uncached = []

      # Create new pages.
      while --pages
        # Create a new leaf page.
        page = @io.createLeaf -(@io.nextAddress++), { loaded: true }

        # Link the leaf page to its siblings.
        page.right = right
        right = page.address

        # Add the address to our parent penultimate branch.
        @io.splice penultimate.page, penultimate.index + 1, 0, page.address

        # Determine the number of records to add to this page from the split
        # leaf. Add an additional record if we have a remainder.
        length = if remainder-- > 0 then records + 1 else records
        offset = split.length - length

        for index in [offset...offset + length]
          # Fetch the record and read it from cache or file.
          position = split.positions[index]
          object = @io.stash(split, position, _)
          @io.uncacheRecord split, position

          # Add it to our new page.
          @io.splice page, page.length, 0, position
          @io.cacheRecord page, position, object.record, object.key

        # Remove the positions that have been merged.
        @io.splice split, offset, length

        # Write the new leaf page to a temporary file.
        @io.rewriteLeaf page, ".replace", _

        replacements.push page
        uncached.push page

      # Link the leaf page to the last created new leaf page.
      split.right = right

      # Write the left most leaf page from which new pages were split.
      @io.rewriteLeaf split, ".replace", _
      replacements.push split

      # Write the branches
      @io.writeBranch penultimate.page, ".pending", _

      # Now rename the last action, committing to our balance.
      @io.rename penultimate.page, ".pending", ".commit", _

      # Rename our files to put them in their place.
      for page in replacements
        @io.replace page, ".replace", _

      # Add our new pages to the cache.
      for page in uncached
        @io.encache page

      # This last replacement will complete the transaction.
      @io.replace penultimate.page, ".commit", _

      # Our left-most and right-most page might be able to merge with the left
      # and right siblings of the page we've just split. We compel a merge
      # detection in the next balance plan by setting the last known size. We do
      # not use the current size, because it is not **known** to be balanced. We
      # cannot employ the split shortcut that only checks for split if a page
      # has grown from being known to be balanced with siblings. Sorry, English
      # bad, but great example. Imagine a page that has been full, but has a
      # sibling that has only one record. We add a record to the full page and
      # split it so that it is half empty. We then add it to the balancer with
      # its half full record count. We want to check for merge and see
      #
      # **TODO**: Swipe &mdash; This always balance until perfect balance is
      # still imperfect.  We may still manage to create a b&#x2011;tree that has
      # leaf pages that alternate from full pages to pages containing a single
      # record, a degenerate case.

      #
      @io.balancer.unbalanced(leaf, true)
      @io.balancer.unbalanced(page, true)

    # Release the pages locked during descent. Seems premature because we're not
    # done yet, but no other descent makes progress unless we invoke a callback.
    @io.unlock(descent.page) for descent in descents

    # Although we've unlocked the penultimate branch page, we know that only the
    # balancer edit a branch page, so we are safe to make a decision about
    # whether to split the penultimate branch page while it is unlocked.
    @shouldSplitBranch penultimate.page, key, _


  splitBranch: ({ key, depth }, _) ->
    descents = []

    # **TODO**: Use address.
    descents.push parent = new Descent(@io)
    parent.descend sought = Descent.key(key), Descent.depth(depth - 1)
    parent.upgrade()

    descend.push child = parent.fork()
    child.descend sought, Descent.depth(depth)

    branchSize = @io.options.branchSize
    pages = Math.ceil(split.length / branchSize)
    remainder = split.length % pages

    replacements = []

    offset = leafSize

    partition = @io.options.branchSize / 2
    right = @io.createBranch @io.nextAddress++, { right: child.page.right }
    child.page.right = right.address
    for index in [partition...child.page.length]
      address = child.page.addresses[index]
      @io.uncacheKey child.page, address
      @io.splice right, right.length, 0, address

    @io.splice child.page, partition, child.page.length - partition
    @io.splice parent.page, parent.index + 1, 0, right.address

    @io.writeBranch root, "pending", _
    @io.writeBranch left, "replace", _
    @io.writeBranch right, "replace", _

    @io.rename root, "pending", "commit", _

    @io.replace left, "replace", _
    @io.replace right, "replace", _
    @io.replace root, "commit", _

    @shouldSplitBranch parent, key, _

  # ### Drain Root
  #
  # When the root branch page is full we don't split it so much as we drain it.
  # We copy the child pages of the root branch page into new branch pages. The
  # new branch pages become the new child pages of the root branch page.
  #
  # This balance operation will increase the height of the b&#x2011;tree. It is
  # the only operation that will increase the height of the b&#x2011;tree.

  # &mdash;
  drainRoot: (_) ->
    # Lock the root. No descent needed.
    root = @io.lock 0, true, _

    # It may have been some time since we've split, so we might have to split
    # into more than two pages.
    pages = Math.ceil(root.length / @io.options.branchSize)
    records = Math.floor(root.length / pages)
    remainder = root.length % pages

    children = []
    right = 0
    while pages--
      # Create a new branch page.
      page = @io.createBranch @io.nextAddress++, { right }

      # Note the right address.
      right = page.address

      # Add the branch page to our list of new child branch pages.
      children.push page

      # Determine the number of records to move from the root branch into the
      # new child branch page. Add an additonal record if we have a remainder.
      length = if remainder-- > 0 then records + 1 else records
      offset = root.length - length

      # Cut off a chunk of addresses.
      cut = @io.splice root, offset, length

      # Uncache the keys from the root branch.
      @io.uncacheKey root, address for address in cut

      # Add the keys to our new branch page.
      @io.splice page, 0, 0, cut

    # Get our children in the right order. We were pushing above.
    children.reverse()

    # Push the child branch page addresses onto our empty root.
    @io.splice root, 0, 0, (page.address for page in children)

    # Write the child branch pages.
    @io.writeBranch page, ".replace", _ for page in children

    # Rewrite our root.
    @io.writeBranch root, ".pending", _

    # Commit the changes.
    @io.rename root, ".pending", ".commit", _

    # Write the child branch pages.
    @io.replace page, ".replace", _ for page in children

    # Commit complete.
    @io.replace root, ".commit", _

    # Add our new children to the cache.
    @io.encache page for page in children

    # Release our lock on the root.
    @io.unlock root

    # Do we need to split the root again?
    @drainRoot _ if root.length > @io.options.branchSize

  deleteGhost: ({ key }, _) ->
    descents = []

    sought = Descent.key(key)

    descents.push pivot = new Descent(@io)
    pivot.descend sought, Descent.found(key), _
    pivot.upgrade()

    descents.push leaf = pivot.fork()
    leaf.descend sought, Descent.leaf, _

    @io.splice leaf.page, 0, 1
    leaf.page.ghosts = 0

    filename = @io.filename leaf.page.address
    fd = fs.open filename, "a", 0o644, _
    position = @io.writePositions fd, leaf.page, _
    fs.close fd, _

    @io.unlock descent.page for descent in descents

  mergeLeaves: ({ key, unbalanced }, _) ->
    # Create a list of descents whose pages we'll unlock before we leave.
    descents = []

    # Descend the tree until we find the key of the leaf page we're going to
    # merge in a branch page.
    descents.push pivot = new Descent(@io)
    pivot.descend(sought = Descent.key(key), Descent.found(key), _)
    pivot.upgrade()

    # Descend to the penultimate page, but first, take note of whether or not
    # the branch page that contains our key is also the penultimate page. We go
    # to the right-most descendant of the left child to find the left leaf page
    # of the merge. We follow the key to find the right leaf page of the merge.
    penultimate = {}

    penultimate.isPivot = pivot.page.addresses[0] < 0

    penultimate.left = pivot.fork()
    penultimate.left.index--
    penultimate.left.descend Descent.right, Descent.penultimate, _

    penultimate.right = pivot.fork()
    penultimate.right.descend sought, Descent.penultimate, _

    # If the leaf page key was found in the penultimate page, then we do not
    # want to key the penultimate pages for release, because they are the same
    # as the branch key page, and the branch key page is already queued for
    # release.
    unless penultimate.isPivot
      descents.push penultimate.left
      descents.push penultimate.right

    # Descend to the leaves. Note that if we're on the penultimate page, the
    # next descent will follow the index we decremented above, the leaf page to
    # the left of the keyed page, instead of going to the right-most leaf page.
    #
    # We use `leftMost` in both cases, because we don't need an index into the
    # leaf page, only a lock on the leaf page.
    leaves = {}

    descents.push leaves.left = penultimate.left.fork()
    leaves.left.descend Descent.leftMost, Descent.leaf, _

    descents.push leaves.right = penultimate.right.fork()
    leaves.right.descend Descent.leftMost, Descent.leaf, _

    # Fix up the index. If the pivot is penultimate, then it is not actually the
    # right-most. TODO: Necessary?
    # if penultimate.isPivot
      # die "eca0523772ceb82cda7d827ebf8e449d", leaves.left.index, leaves.right.index, leaves.left.page.address, penultimate.left.page.address
      # leaves.left.index = leaves.right.index - 1

    # Determine if we still have candidates for merge.
    if leaves.left.page.length - leaves.left.page.ghosts + leaves.right.page.length - leaves.right.page.ghosts <= @io.options.leafSize
      # Uncache the pivot key.
      @io.uncacheKey pivot.page, pivot.page.addresses[pivot.index]

      # The right leaf page of of the merged page is the right leaf page of the
      # right page of the merge.
      leaves.left.page.right = leaves.right.page.right

      # Append all of the records of the right leaf page, excluding any ghosts.
      for index in [leaves.right.page.ghosts...leaves.right.page.length]
        # Fetch the record and read it from cache or file.
        position = leaves.right.page.positions[index]
        object = @io.stash leaves.right.page, position, _
        @io.uncacheRecord leaves.right.page, position

        # Add it to our new page. The negative positions are temproary. We'll
        # get real file positions when we rewrite.
        @io.splice leaves.left.page, leaves.left.page.length, 0, -(position + 1)
        @io.cacheRecord leaves.left.page, -(position + 1), object.record, object.key

      # Remove the positions the outgoing page to update the JSON size of the
      # b&#x2011;tree.
      @io.splice leaves.right.page, 0, leaves.right.page.length

      # Rewrite the left leaf page. Move the right leaf page aside for the
      # pending unlink.
      @io.rewriteLeaf leaves.left.page, ".replace", _
      @io.rename leaves.right.page, "", ".unlink", _

      @io.uncacheKey pivot.page, pivot.page.addresses[pivot.index]
      @io.splice penultimate.right.page, penultimate.right.index, 1

      @io.writeBranch penultimate.right.page, ".pending", _

      # **TODO**: If I succeed, how will I know to test the parents for balance?
      # **TODO**: Uh, can't the medic just note that this page needs to be
      # rebalanced? It can force a propagation of balance and merge checking of
      # the parent.

      # Renaming pending to commit will cause the merge to roll forward.
      @io.rename penultimate.right.page, ".pending", ".commit", _

      @io.replace leaves.left.page, ".replace", _
      @io.unlink leaves.right.page, ".unlink", _
      @io.replace penultimate.right.page, ".commit", _

    # We cannot merge, so we queue one or both of pages for a merge test on the
    # next balancer.
    else
      die "3c49b727e770705625e625f7ddca5405"
      if unbalanced[leaves.left.page.address]
        die "133bb6b9d2e789d073f00ae19caf62ee"
        @io.balancer.unbalanced(leaves.left.page, true)
      if unbalanced[leaves.right.page.address]
        die "139419730d062be5ac0c3826e5e96bb2"
        @io.balancer.unbalanced(leaves.right.page, true)

    @io.unlock descent.page for descent in descents

  mergeBranches: ({ key, address }, _) ->
    descents = new Descent(@io)

    # Roughly: First choose which to merge. Then merge.
    #
    # I believe that, once we find the key, we have found

    # We need to go left then right when we see the key.
    descent = new Descent(@io)
    descent.descend sought = descent.key(key), descent.found(key), _
    descent.index--
    descent.descend Descent.right, descent.depth(depth - 1), _
    # The binary search will put us at the last index. We are now where we need
    # to be.
    descent.descend sought, descent.depth(depth), _

    # Get the page.
    pages = [ page = descent.page ]

    # Prevent purge?
    page.balancing = true

    while page.right and pages.length < 3
      pages.push page = @io.lock page.right, false, _

    branchSize = @io.options.branchSize

    if pages.length > 1 and pages[0].length + pages[1].length < branchSize
      merge = [ pages[0].address, pages[1].address ]
    else if pages.length is 3 and pages[1].length + pages[2].length < branchSize
      merge = [ pages[1].address, pages[1].address ]

    @io.unlock page for page in pages

    return unless merge

    page = @io.lock merge[0], false, _
    key = @io.key page, 0, _
    @io.unlock page

    descents = []
    sought = Descent.key(key)

    descents.push pivot = new Descent(@io)
    pivot.descend sought, descent.found(key)

    parent = {}

    parent.left = pivot.fork()
    parent.left.descend Decent.right, Decent.depth(depth - 1)

    parent.right = piviot.fork()
    parent.right.descend sought, Decent.depth(depth - 1)

    parent.isPivot = parent.right.depth is pivot.depth

    unless parent.isPivot
      descents.splice(0, 0, parent.left, parent.right)

    child = {}

    descents.push child.left = parent.left.fork()
    child.left.descend sought, Decent.depth(depth)
    child.left.index-- if parent.isPivot

    descents.push child.right = parent.right.fork()
    child.right.descend sought, Decent.depth(depth)

    for address in child.right.page.addresses
      child.left.page.addresses.push address

    @io.size -= child.right.page.size

    @io.writeBranch child.left.page, "replace", _

    child.right.page.addresses.length = 0
    @io.writeBranch child.right.page, "unlink", _

    if parent.isPivot
      right = pivot.page
      right.positions.splice(pivot.index, 1)
    else
      right = parent.right.page
      unless right.addresses.length is 1
        @io.uncacheKey right, right.addresses[1]
      right.addresses.shift()

    @io.writeBranch right, "pending", _

    # **TODO**: If I succeed, how will I know to test the parents for balance?
    # Got to think all over again in medic about who is whose parent?
    @io.rename right, "pending", "commit", _

    @io.replace child.left.page, "replace", _
    @io.unlink child.right.page, "unlink", _
    @io.replace right, "commit", _

    # We only check to fill the root if there are only two nodes in the root
    # that can be combined to fill the root.
    if right.address is 0 and right.length is 2
      @operations.unshift method: "fillRoot"
    else
      @operations.unshift
        method: "mergeBranches"
        key: @io.key(right, 0, _)
        depth: depth - 1

    @io.unlock descent.page for descent in descents

  fillRoot: ({}, _) ->
    pages = []

    pages.push root = @io.lock 0, true, _
    pages.push left = @io.lock root.addresses[0], true, _
    pages.push right = @io.lock root.addresses[1], true, _

    if left.length + right.length <= @io.options.branchSize
      @io.uncache page for page in pages
      root.addresses = left.addresses.concat(right.addresses)

    @io.writeBranch left, "unlink", _
    @io.writeBranch right, "unlink", _

    @io.writeBranch root, "pending", _

    @io.rename root, "pending", "commit", _

    @io.unlink page, "unlink", _ for page in [ left, right ]
    @io.replace root, "commit", _

    @io.unlock page for page in pages

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

  # The `key` is the splat array passed to `Strata.iterator` or
  # `Strata.mutator`. If it is zero length, that means no argument was passed,
  # indicating that we should place the cursor at first element in the entire
  # tree. Otherwise, it is the key of the record or insert location to find.
  #
  # We use the length of the splat, instead of a existence check, so that the
  # application developer can use `null` as a key, even though no one should
  # ever use `null` as a key directly. Use a pseudo-duplicate `null` instead.

  # &mdash;
  _cursor: (key, exclusive, constructor, _) ->
    # In theory, we can support null keys, since we can test to see if we've
    # been provided a key value by the arity of invocation.
    sought = if key.length then Descent.key(key[0]) else Descent.leftMost

    # Descend to the penultimate branch page.
    descent = new Descent(@_io)
    descent.descend(sought, Descent.penultimate, _)
    descent.exclude() if exclusive

    descent.descend(sought, Descent.leaf, _)

    new constructor(key, descent)

  iterator: (splat..., callback) ->
    @_cursor splat, false, Iterator, callback

  mutator: (splat..., callback) ->
    @_cursor splat, true, Mutator, callback

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
