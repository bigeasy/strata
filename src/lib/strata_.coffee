# A Streamline.js friendly evented I/O b-tree for node.js.
#
# ## Purpose
#
# Strata stores JSON objects on disk, according to a sort order of your
# choosing, indexed for fast retrieval. Faster than a flat file. Lighter than a
# database. More capacity than an in memory tree.
# 
# Strata is a [b-tree](http://en.wikipedia.org/wiki/B-tree) implementation for
# [Node.js](http://nodejs.org/) that **evented**, **concurrent**, **perstant**
# and **durable**.
#
# Strata is **evented** because it uses asynchronous I/O to read and write
# b-tree pages, allowing your CPU to continue to do work while Strata waits on
# I/O.
#
# Strata is **concurrent**. Strata will satisfy any queries from its in memory
# cache that it can, even while waiting on evented I/O to load. It queues the
# I/O, but keeps the main thread of execution busy while the I/O completes.
# Strata keeps your CPU and I/O busy when requests are heavy.
#
# Strata is **persistant**. It stores your tree in page files. The page files
# are plain old JSON. You can open them up in a `vim` or EMACS, back them up
# using `textutils`, check them into `git`, and munge them with JavaScript. It's
# your data, you ought to be able to read it.
#
# Strata is **durable**. It only appends records to to file, so a hard shutdown
# will only ever lose the few last records added. When pages jornaled when they
# are vaccumed and rewritten.
#
# Strata is a b-tree. A b-tree is a database primiative. Using Strata, you can
# start to experiment with database designs of your own. You can use Strata to
# build an MVCC database table, like PostgreSQL. You can create Strata b-trees
# to create relationship indexes. You can use Strata to store what ever form of
# JSON suits your needs like NoSQL databases.
#
# It runs anywhere that Node.js runs, in pure JavaScript.
#
# Maybe you need a database server, or maybe Strata is all you need to get your
# next application growing.
#
# ## Terminology
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
# ## What is a b-tree?
#
# This documentation assumes that you understand the theory behind the b-tree,
# and know the variations of implementation. If you are interested in learning
# about b-trees, this document could be a learning aid for you, but you should
# start with the Wikipedia articles on
# [B-trees](http://en.wikipedia.org/wiki/B-tree) and
# [B+trees](http://en.wikipedia.org/wiki/B%2B_tree). 
#
# ## What flavor of b-tree is this?
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
# ## Implementation
#
# The b-tree has two types of pages. Leaf pages and branch pages.
#
# ### Leaf Pages
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
# ### Branch Pages
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
# #### Root Branch Page
#
# The root page is the first page we consult to find the desired leaf page. Our
# b-tree always contains a root page. The b-tree is never so empty that the root
# page disappears. The root page always has the same address.
#
# TK move. Until the root branch page is split, it is both the root branch page
# and a penultimate branch page.
#
# #### Penultimate Branch Pages
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
# #### Interior Branch Pages
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
# ### Keys and First Records
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

# ## Page Storage
#
# A *leaf page file* contains insert objects, delete objects and address array
# objects, stored as JSON, one object per line, as described above. The JSON
# objects stored on behalf of the client are called *records* and they are
# contained within the insert objects.
#
# A *branch page file* contains a single JSON object stored on a single line
# that contains the array of child page addresses.
#
# The `IO` class manages the wholesale reading and writing of page files. The
# `RecordIO` class manages the insertion and deletion of individual records in a
# leaf page file.

#
class IO
  # Each page is stored in its own file. The files are all kept in a single
  # directory. The directory is specified by the client when the database object
  # is constructed.
  #
  # The `IO` class needs the `extractor` to extract keys from records. It does
  # not store the `extractor`, of course.

  # Set directory and extractor. Initialze the page cache and MRU list.
  constructor: (@directory, @extractor) ->
    @cache          = {}
    @head           = { address: -1 }
    @head.next      = @head
    @head.previous  = @head
    @nextAddress    = 0
    @length         = 1024

  # Pages are identified by an integer page address. The page address is a number
  # that is incremented as new pages are created. A page file has a file name that
  # includes the page address.  When we load a page, we first derive the file name
  # from the page address, then we load the file.
  #
  # The `filename` method accepts a suffix, so that we can create replacement
  # files. Instead of overwriting an existing page file, we create a replacement
  # with the suffix `.new`. We then delete the existing file and rename the
  # replacement file using the `relink` method. This two step write is part of
  # our crash recovery strategy.
  #
  # We always write out entire branch page files. Leaf pages files are updated
  # by appending, but on occasion we rewrite them to vaccum deleted records.

  # Create a file name for a given address with an optional suffix.
  filename: (address, suffix) ->
    suffix or= ""
    padding = "00000000".substring(0, 8 - String(address).length)
    "#{@directory}/segment#{padding}#{address}#{suffix}"

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
  allocateBranches: (penultimate) ->
    address = @nextAddress++
    link = @link({ penultimate, address, addresses: [], cache: {} })
    @cache[address] = link

  # We write the branch page to a file as a single JSON object on a single line.
  # We tuck the page properties into an object, and then serialize that object.
  # We do not store the branch page keys. They are looked up as needed as
  # described in the b-tree overview above.
  #
  # We always write a page branch first to a replacement file, then move it
  # until place using `relink`.

  #
  rewriteBranches: (page, _) ->
    filename = @filename page.address, ".new"
    record = [ page.penultimate, page.next?.address, page.addresses ]
    json = JSON.stringify(record) + "\n"
    fs.writeFile filename, json, "utf8", _

  # To read a branch page we read the entire page and evaluate it as JSON. We
  # did not store the branch page keys. They are looked up as needed as
  # described in the b-tree overview above.

  #
  readBranches: (page, _) ->
    filename = @filename page.address
    json = fs.readFile filename, "utf8", _
    [ penultimate, next, addresses ] = JSON.parse json
    extend page, { penultimate, next, addresses }

  # ### Page Caching
  #
  # We keep a map of page addresses to cached page objects.
  #
  # We also maintain a most-recently used linked list using the page objets as
  # list nodes. When we want to cull the cache, we can remove the pages at the
  # end of the list, since they are the least recently used.
  #
  # TODO Some way of judging popularity. A decent index?
  #
  # It is important that the page objects are unique, that we do not represent a
  # page file with more than one page object, because the page objects house the
  # locking mechanisms.

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

  # ### Leaf Tier Files
  #
  # A leaf tier maintains an array of file positions called a positions array. A
  # file position in the positions array references a record in the leaf tier
  # file by its file position. The positions in the positions array are sorted
  # according to the b-tree collation of the referenced records.
  #
  # A new leaf page is given the next unused page number.
  #
  # The in memory representation of the leaf page includes a flag to indicate
  # that the page is leaf page, the address of the leaf page, the page address
  # of the next leaf page, and a cache that maps record file positions to
  # records that have been loaded from the file.

  #
  allocateLeaves: (positions, right) ->
    address = @nextAddress++
    @cache[address] = @link
      leaf: true
      address: address
      positions: positions
      cache: {}
      right: right

  # #### Appends for Durability
  #
  # A leaf tier file contains JSON objects, one object on each line. The objects
  # represent record insertions and deletions, so that the leaf tier file is
  # essentially a log. Each time we write to the log, we open and close the
  # file, so that the operating system will flush our writes to disk. This gives
  # us durability.
  
  # Append an object to the leaf tier file as a single line of JSON.
  #
  # We call the append method to both update an existing leaf page file, as
  # well as to create a replacment leaf page file that will be relinked to
  # replace the existing leaf page file. The caller determines which file should
  # be written, so it opens and closes the file descriptor.
  #
  # The file descriptor must be open for for append. 

  #
  _writeJSON: (fd, page, object, _) ->
    # TODO Move this point somewhere. Since we need to fsync anyway, it we just
    # to open the file and and close the file when we append a JSON object to
    # it. Because no file handles are kept open, the b-tree object can simply be
    # reaped by the garbage collection.
    page.position or= fs.fstat(fd, _).size

    # Calcuate a buffer length. Take note of the current page position.
    json            = JSON.stringify object
    position        = page.positon
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

  # #### Leaf Tier File Records
  #
  # There are three types of objects in a leaf tier file, *insert objects*,
  # *delete objects*, and *address array objects*.
  #
  # An insert object contains a *record* and the index in the address array
  # where the record's address would be inserted to preserve the sort order of
  # the address array.
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

  # Write an insert object.
  writeInsert: (fd, page, object, index, _) ->
    @_writeJSON fd, page, [ index + 1, object ], _

  # If the first element is less than zero, it indicates a delete object. The
  # absolute value of the integer is the one based index into the zero based
  # position array, indicating the index of address array element that should be
  # deleted.
  #
  # There are no other elements in the delete object.

  # Write a delete object.
  writeDelete: (fd, page, index, _) ->
    @_writeJSON fd, page, [ -(index + 1) ], _
  
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
  readLeaves: (page, _) ->
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
            [ positions, end ] = [ record, 0 ]
            break
          else
            splices.push [ index, start + read ]
            if index > 0
              cache[start + read] = record.shift()
      eol = start + eos
    # Now we replay the inserts and deletes described by the insert and delete
    # objects that we've gathered up in our splices array.
    splices.reverse()
    for splice in splices
      [ index, address ] = splice
      if index > 0
        positions.splice(index - 1, 0, address)
      else
        positions.splice(-(index + 1), 1)
    # Close the file descriptor.
    fs.close fd, _
    # Return the loaded page.
    extend page, { addresses, cache, loaded: true }

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
  # the shutdown of the b-tree. As it stands, we can always simply let the
  # b-tree succumb to the garbage collector, because we hold no other system
  # resources that need to be explictly released.
  #
  # Note how we allow we keep track of the minimum buffer size that will
  # accommodate the largest possible buffer.
  #
  # TODO Have a mininum buffer that we constantly reuse, uh no. That will be
  # shared by descents.

  #
  readRecord: (page, address, _) ->
    filename = @filename page.address
    page.position or= fs.stat(filename, _).size
    fd = fs.open filename, "r", _
    loop
      buffer = new Buffer(@length)
      read = fs.read fd, buffer, 0, buffer.length, address, _
      if json = @_readJSON(buffer, read)
        break
      if @length > page.position - address
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
      position = @writeInsert fd, page, object, index, _
      positions.push position
      cache[position] = object
    extend page, { positions, cache }
    @writePositions fd, page, _
    fs.close fd, _

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
  create: (_) ->
    # Create the directory if it doesn't exist.
    stat = fs.stat @directory, _
    if not stat.isDirectory()
      throw new Error "database #{@directory} is not a directory."
    if fs.readdir(@directory, _).length
      throw new Error "database #{@directory} is not empty."
    # Create a root branch with a single empty leaf.
    root = @allocateBranches true
    leaf = @allocateLeaves([], -1)
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
  # load it, and our `readLeaves` function will check for bad JSON, finding it
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
  # The b-tree must only be read and written by a single Node.js process. It is
  # not suitable for use with multiple node processes, or the cluster API.
  #
  # Although there is only a single thread in a Node.js process, the b-tree is
  # still a concurrent data structure. Instead of thinking about concurrency in
  # terms of threads we talk about concurrent *descents* of the b-tree.
  #
  # When we search the tree or alter the tree, we must descend the tree.
  #
  # Decents of the b-tree can become concurrent when descent encounters a page
  # that is not in memory. While it is waiting on evented I/O to load the page
  # files, the main thread of the process can make progress on another request
  # to search or alter the b-tree, it can make process on another descent.
  #
  # This concurrency keeps the CPU and I/O loaded.
  #
  # ### Locking
  #
  # Locking prevents race conditions where an evented I/O request returns to to
  # find that the sub-tree it was descending has been altered in way that causes
  # an error. Pages may have split or merged by the main thread, records may
  # have been inserted or deleted. While evented I/O is performed, the sub-tree
  # needs to be locked to prevent it from being altered.
  #
  # The b-tree is locked page by page. We are able to lock only the pages of
  # interest to a particular descent of the tree.
  #
  # Futhermore, the b-tree destinguishes between shared read locks and exclusive
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

  #
  lock: (exclusive, address, leaf, callback) ->
    # We must make sure that we have one and only one page object to represent
    # the page. We the page object will maintain the lock queue for the page. It
    # won't due to have different descents consulting different lock queues.
    # There can be only one.
    #
    # The queue is implemented using an array of arrays. Shared locks are
    # grouped inside one of the arrays in the queue element. Exclusive locks are
    # queued alone as a single element in the array in the queue element.
    if not page = @cache[address]
      @cache[address] = page =
        @link
          leaf: leaf
          address: address
          cache: {}
          addresses: []
          locks: [[]]

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
        else if leaf
          @readLeaves page, callback
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
        lock(null, tier)
    else
      locks[locks.length - 1].push lock
      if locks.length is 1
        lock(null, tier)

  # #### Unlock

  # When we release a lock, we simply shift a callback off of the array in the
  # first element of the queue to decrement the lock count. We are only
  # interested in the count, so it doesn't matter if the callback shifted by the
  # descent is the one that it queued.

  #
  release: (tier) ->
    locks = tier.locks
    running = locks[0]
    running.shift()
    say { locks }
    if running.length is 0 and locks.length isnt 1
      locks.shift()
      @resume tier, locks[0] if locks[0].length

  # We call resume with the list of callbacks shifted off of a pages lock queue.
  # The callbacks are scheduled to run in the next tick.
  resume: (page, continuations) ->
    process.nextTick =>
      for callback in continuations
        callback(null, page)
      
  upgrade: (tier, _) ->
    @release tier
    @lock true, tier.address, false, _

# Descent of a tree.

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

  # Going forward, every will story keys, so that when we encounter a record, we
  # don't have to run the extractor, plus we get some caching.
  #
  # Or maybe just three arrays, key, object and address? I'm talking about using
  # each key as an object cache, by adding an object member to the key. But,
  # they cache container is a hash table, which is a compactish representation,
  # so why not make it yet anohter hash table? The logic gets a lot easier.
  #
  # TODO None of this is right.
  record: (page, index, _) ->
    position = page.positions[index]
    if not record = page.cache[position]
      record = page.cache[position] = @readRecord page, position, _
    record

  # TODO Descend. Ah, well, find is in `Mutation`, so this moves to `Mutation`.
  _key: (tierOrAddress, index, _) ->
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
