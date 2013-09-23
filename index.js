// An evented I/O b&#x2011;tree for Node.js.
//
// TK Define ***least child page***.
//
// ## Purpose
//
// Strata stores JSON objects on disk, according to a sort order of your
// choosing, indexed for fast retrieval.
//
// Strata is faster than a flat file and lighter than a database, with more
// capacity than an in&#x2011;memory tree.
//
// Strata is a [b&#x2011;tree](http://en.wikipedia.org/wiki/B-tree)
// implementation for [Node.js](http://nodejs.org/) that is **evented**,
// **concurrent**, **persistent** and **durable**.
//
// Strata is **evented**. It uses asynchronous I/O to read and write
// b&#x2011;tree pages, allowing your CPU to continue to do work while Strata
// waits on I/O.
//
// Strata is **concurrent**. Strata will answer any queries from its
// in&#x2011;memory cache when it can, so requests can be satisfied even when
// there are evented I/O requests outstanding.
//
// Strata is **persistent**. It stores your tree in page files. The page files
// are plain old JSON text files that are easy to manage. You can view them
// with `less` or `tail -f`, back them up hot using `rsync`, and version them
// with `git`.
//
// Strata is **durable**. It only appends records to a file, so a hard shutdown
// will only ever lose the few last records added. Pages are journaled when they
// are vacuumed or rewritten.
//
// Strata is a b&#x2011;tree. A b&#x2011;tree is a database primitive. Using
// Strata, you can start to experiment with database designs of your own. You
// can use Strata to build an MVCC database table, like PostgreSQL. You can
// create Strata b&#x2011;trees to create indexes into structured data that is
// not already in a database, like monster log files. You can use Strata to
// store your data in what ever form of JSON suits you, like a NoSQL database.
//
// As a bonus, Strata is two database primitives in one, because with a time
// series index, you can use Strata as a write&#x2011;ahead log.
//
// Strata runs anywhere that Node.js runs, in pure JavaScript.
//
// ## Collaboration
//
// Documentation for Strata is presented here, in literate programming style.
// API documentation is not yet available, so scoot down to the `Strata` class
// for an in-depth look at the API. Have a look at the extensive test suite for
// examples on the different operations.
//
// If you find a bug with Strata, please report it at the [GitHub
// Issues](https://github.com/bigeasy/strata/issues). If you want to leave a
// comment on the documentation or the code, you can ping me, Alan Gutierrez, at
// @[bigeasy](https://twitter.com/#!/bigeasy) on Twitter.
//
// Feel free to fork and explore. Note that Strata is a database primitive, not
// a database in itself. Before you fork and add features you feel are missing,
// please consult with me. Perhaps your ideas are better expressed as a project
// that employs Strata, instead of as a patch to Strata itself.
//
// ## Terminology
//
// We refer to the nodes in our b&#x2011;tree as ***pages***. The term node
// conjures an image of a discrete component in a linked data structure that
// contains one, maybe two or three, values. Nodes in a b&#x2011;tree contain
// hundreds or thousands of values. They are indexed. They are read from disk.
// They are allowed to fall out of memory when they have not been recently
// referenced. These are behaviors that people associate with a page of values.
//
// Otherwise, we use common terminology for ***height***, ***depth***,
// ***parent***, ***child***, ***split*** and ***merge***.
//
// There is no hard and fast definition for all the terms. A leaf is a fuzzy
// concept in b&#x2011;tree literature, for example. We call a page that
// contains records a ***leaf page***. We call a non-leaf page a ***branch
// page***. The term ***order*** means different things to different people.  We
// define the order of a branch page to be the maximum number of child pages,
// while the order of a leaf page to be the maximum number of records.
//
// We refer to the actual number of child pages in a branch page or the actual
// number of records in a leaf page as the page ***length***.
//
// The term ***b&#x2011;tree*** itself may not be correct. There are different
// names for b&#x2011;tree that reflect the variations of implementation, but
// those distinctions have blurred over the years. Our implementation may be
// considered a b+tree, since pages are linked, and records are stored only in
// the leaves.
//
// Terms specific to our implementation will be introduced as they are
// encountered in the document.
//
// ## Documentation Style
//
// This documentation is written to be read through, with no expecation that
// you, dear reader, will actually read it though. It is merely the style of the
// documentation. When you jump to read the documentation specific to
// a block of code, you're liable to read references to "the above", or be asked
// to "see below." The referenced passage should not be too far away, but you'll
// have go back or keep reading to find it.
//
// The documentation is not meant to be a handy reference to the interface, but
// instead an essay on logic behind the implementation. The alternative is to
// repeat descriptions of difficult concepts every time they are visited by the
// code. This is going to be painfully dull for the person who gives the code
// the time and attention necessary for a proper understanding.
//
// **TODO**: Remove this paragraph once you get used to the notion that people
// are probably going to mewl about not being able to understand the code, want
// an API reference, etc.
//
// ## What is a b&#x2011;tree?
//
// This documentation assumes that you understand the theory behind the
// b&#x2011;tree, and know the variations of implementation. If you are
// interested in learning about b&#x2011;trees you should start with the
// Wikipedia articles on [B-trees](http://en.wikipedia.org/wiki/B-tree) and
// [B+trees](http://en.wikipedia.org/wiki/B%2B_tree). I was introduced to
// b&#x2011;trees while reading [Algorithms in
// C](http://www.amazon.com/dp/0201314525), quite some time ago.
//
// ## What flavor of b&#x2011;tree is this?
//
// Strata is a b&#x2011;tree with leaf pages that contain records ordered by the
// collation order of the tree. Records are stored for retrieval in constant
// time, addressed by an integer index so that they can be found using binary
// search.
//
// Branch pages contain links to other pages, and do not store records
// themselves.
//
// Leaf pages are linked in ascending order to simplify the implementation of
// traversal by cursors. Branch pages are singly linked in ascending order to
// simplify implementation of branch page merges.
//
// The order of a branch page is the maximum number of children for a branch
// page. The order of a leaf page is the maximum number of records for a leaf page.
// When a page exceeds its order it is split into two or more pages. When two
// sibling pages next to each other can be combined to create a page less than
// than the order, they are merged.
//
// The b&#x2011;tree always has a root branch page. The height of the tree
// increases when the root branch page is split. It decreases when the root
// branch page is merged. The split of the root branch is a different operation
// from the split of a non-root branch, because the root branch does not have
// siblings.

// Copy values from one hash into another.
function extend(to, from) {
  for (var key in from) to[key] = from[key];
  return to;
}

// Used to manipulate `argument` arrays.
var __slice = [].slice;

/*function say () {
    var args = __slice.call(arguments);
    console.log(require('util').inspect(args, false, null));
}*/

// ## Collation
//
// A b&#x2011;tree has a collation defined by the application developer.
//
// The collation is determined by the combination of an extractor and a
// comparator. The extractor is used to extract a key from the stored record.
// The comparator is used to order records by comparing the key.
//
// Separating extraction from comparison allows us to cache the key. We do not
// need the whole record for comparison, only the key. Keys are used to order
// the tree, so when we will constantly be reading records off the disk solely
// to get their key value.
//
// If a record is read for its key, but the record is not soon visited by a
// cursor, it will eventually be collected by a cache purge. If the key is
// frequently consulted by queries as they descend the tree, the key will be
// retained. If the key is subset of a large record, purging the records and
// retaining the keys will reduce the in&#x2011;memory size of the
// b&#x2011;tree.
//
// Also, the comparator is pretty easily generalized, while the extractor is
// invariably specialized. You might have a single string comparator that you
// use with extractors specialized for different types of records.
//
// ### Default Collation
//
// You will almost certainly define your own extractors and comparators, but the
// b&#x2011;tree has a default that works for a b&#x2011;tree that stores only
// JavaScript primitives.

// Default comparator for JavaScript primitives. You can use `a - b` for
// numbers. This works with both strings and numbers.
function compare (a, b) { return a < b ? -1 : a > b ? 1 : 0 }

// Default extractor returns the value as whole for use as a key.
function extract (a) { return a }

// I began this project long ago, in 2010, as a first Node.js project, and as a
// first Node.js project, I experimented with the closure style of object
// construction. All of a `Strata` is a closure over the implementation. No
// prototypes. Instead, I build objects on the fly using `classify`.
//
// This `classify` function builds an object from a list of functions, assigning
// the functions as member functions, or else defining them as property getters
// or setters. If the function name starts with a `_`, then the name without the
// underscore is used as a property getter. If the function name ends with a
// `_`, then the property name is used as a property setter.
function classify () {
  var i, I, name;
  for (i = 0, I = arguments.length; i < I; i++) {
    name = arguments[i].name;
    if (name[0] == "_")
      this.__defineGetter__(name.slice(1), arguments[i]);
    else if (name[name.length - 1] == "_")
      this.__defineSetter__(name.slice(0, name.length - 1), arguments[i]);
    else
      this[arguments[i].name] = arguments[i];
  }
  return this;
}

// ## Pages
//
// Our b&#x2011;tree has two types of pages: leaf pages and branch pages.
//
// A ***leaf page*** contains records. A ***branch page*** contains references
// to other pages.
//
// Both leaf pages and branch pages are ordered according to the collation.
//
// To find a record, we descend a tree of branch pages to find the leaf page
// that contains the record. That is a b&#x2011;tree.
//
// ### Page I/O
//
// The `IO` class manages the reading and writing of leaf and branch pages to
// and from disk, page locking and page caching. It also implements the binary
// search we use to search the pages.

// ### Checksummed Lines of JSON

// Each page is stored in its own ***page file***. The page files are all kept
// in a single directory. The directory is specified by the application
// developer when the `Strata` object is constructed.
//
// Page files contain one or more JSON strings, one string per line. The JSON
// strings are checksummed. The checksum follows the JSON string on the line.
// The checksum is written as a hexadecimal number.
//
// The line based JSON format plays nice with traditional UNIX text utilities.
//
// A ***leaf page file*** acts as journal recording edit events. A JSON string
// is appended to the leaf page file to record a record insert or delete.
//
// A ***branch page file*** contains a single JSON object stored on a single
// line that contains the array of child page addresses.
//
// **TK**: Documentation damage. We're now calling a reference array a position
// array in a leaf page and an address array in a branch page. Do we want to
// consolidate to reference array? I'm beginning to think so. Many actions on
// this array are the same for both leaf pages and branch pages.
//
// When we read records and record keys off the disk, we store them in an object
// that acts as a cache for the page. The in-memory page object contains an
// array of integers that act as either page addresses or record positions. We
// call this the ***reference array***. The integers are stored in the reference
// array in the collation order of the stored records they reference.
//
// The in memory page object also contains a map of integer addresses to JSON
// objects. This is the ***record cache*** for the page object. The integers in
// the reference array are always present when the page is loaded, so that the
// integer array is dense, but the record cache only contains entries for
// records that have been referenced. We use a binary search to probe for keys
// and records, so we can avoid loading records we don't need.
//
// **TK**: Damage mentioned above ends here.
//
// We count on our JavaScript array implementation to be [clever about memory
// usage](http://stackoverflow.com/questions/614126/why-is-array-push-sometimes-faster-than-arrayn-value/614255\#614255).

// Set directory and extractor. Initialize the page cache and most-recently used
// list.
//
// The checksum will become CRC 32 by default in future releases and
// configurable to use any of the `crypto` module hashes or no checksum at all.

//
function Strata (options) {
  var directory = options.directory
    , extractor = options.extractor || extract
    , comparator = options.comparator || compare
    , fs = options.fs || require('fs')
    , path = options.path || require('path')
    , ok = function (condition, message) { if (!condition) throw new Error(message) }
    , cache = {}
    , mru = { address: null }
    , nextAddress = 0
    , length = 1024
    , balancer = new Balancer()
    , balancing
    , size = 0
    , hash = options.checksum || { algorithm: "sha1", count: 0 }
    , checksum
    , crypto
    , constructors = {}
    , tracer = options.tracer || function () { arguments[2]() }
    ;

  switch (hash.algorithm) {
  case "none":
    checksum = function () { return 0 }
    break;
  default:
    crypto = require("crypto");
    checksum = function (m) { return crypto.createHash(hash.algorithm).update(m).digest("hex") }
  }

  function validator (callback) {
    return function (forward, type, report) { return validate(callback, forward, type, report) }
  }

  var thrownByUser

  function validate (callback, forward, type, report) {
    ok(typeof forward == "function", 'no forward function');
    ok(typeof callback == "function",'no callback function');
    return function (error) {
      if (error) {
        toUserLand(callback, error);
      } else {
        try {
          if (type && report) {
            tracer(type, report, validate(callback, forward));
          } else {
            forward.apply(null, __slice.call(arguments, 1));
          }
        } catch (error) {
          // Do not catch an exception thrown by the user.
          if (thrownByUser === error) {
            throw error
          }
          toUserLand(callback, error);
        }
      }
    }
  }

  // When we call a user's callback, we wrap our call. If the user has thrown an
  // exception, we make sure that `validate` will not catch it and feed it back
  // to the user as a Strata-generated Error.
  function toUserLand (callback) {
    try {
      callback.apply(null, __slice.call(arguments, 1))
    } catch (error) {
      thrownByUser = error
      throw error
    }
  }

  function _size () { return size }

  function _nextAddress () { return nextAddress }

  mru.next = mru.previous = mru;

  function report () {
    return {
      size: size,
      nextAddress: nextAddress,
      cache: Object.keys(cache)
    }
  }

  // #### Verifying Checksums

  // When we read a line from a branch page file or a leaf page file, we always
  // verify the checksum. We use the checksum algorithm specified in the
  // `Strata` constructor.
  //
  // In our branch page files and leaf page files, we store one JSON string
  // per line. The checksum is written as a hexadecimal number following the
  // JSON string. We checksum the JSON string and compare it to the stored
  // checksum.
  //
  // A hyphen stored in place of the hexadecimal indicates no checksum.

  //
  function readLine (line) {
    var match = /^\s?(.*)\s((?:-|[\da-f]+))\s?$/i.exec(line);
    ok(match, "corrupt line: cannot split line: " + line);
    ok(match[2] == "-" || checksum(match[1]) == match[2], "corrupt line: invalid checksum");
    return JSON.parse(match[1])
  }

  // Pages are identified by an integer page address. The page address is a
  // number that is incremented as new pages are created. A page file has a file
  // name that includes the page address.  When we load a page, we first derive
  // the file name from the page address, then we load the file.
  //
  // **TK**: Rewrite once we've finalized journaled balancing.
  //
  // The `filename` method accepts a suffix, so that we can create replacement
  // files. Instead of overwriting an existing page file, we create a
  // replacement with the suffix `.new`. We then delete the existing file with
  // the `delete` method and move the replacement into place with the `replace`
  // method. This two step write is part of our crash recovery strategy.
  //
  // We always write out entire branch page files. Leaf pages files are updated
  // by appending, but on occasion we rewrite them to vacuum deleted records.

  // Create a file name for a given address with an optional suffix.
  function filename (address, suffix) {
    suffix || (suffix = "");
    return path.join(directory, address + suffix)
  }

  // Move a replacement page file into place. Unlink the existing page file, if
  // it exists, then rename the replacement page file to the permanent name of
  // the page file.
  function replace (page, suffix, callback) {
    var replacement = filename(page.address, suffix)
      , permanent = filename(page.address)
      ;

    fs.stat(replacement, validator(callback)(stat));

    function stat (stat) {
      ok(stat.isFile(), 'is not a file');
      fs.unlink(permanent, unlinked);
    }

    function unlinked (error) {
      if (error && error.code != "ENOENT") callback(error);
      else fs.rename(replacement, permanent, callback);
    }
  }

  // Rename a page file from a page file with one suffix to another suffix.
  function rename (page, from, to, callback) {
    fs.rename(filename(page.address, from), filename(page.address, to), callback);
  }

  // Unlink a page file with the given suffix.
  function unlink (page, suffix, callback) {
    fs.unlink(filename(page.address, suffix), callback);
  }

  // ### Page Caching
  //
  // We keep an in&#x2011;memory map of page addresses to page objects. This is
  // our ***page cache***.
  //
  // #### Most-Recently Used List
  //
  // We also maintain a ***most-recently used list*** as a linked list using the
  // page objects as list nodes. When we reference a page, we unlink it from the
  // linked list and relink it at the head of the list. When we want to cull the
  // cache, we can remove the pages at the end of the linked list, since they
  // are the least recently used.
  //
  // #### Cache Entries
  //
  // Cache entries are the page objects themselves.
  //
  // It is important that the page objects are unique and that we do not represent
  // a page file with more than one page object, because the page objects house
  // the locking mechanisms. The page object acts as a mutex for page data.
  //
  // The cache entries are linked to form a doubly-linked list. The
  // doubly-linked list of cache entries has a head node that has a null
  // address, so that end-of-list traversal is unambiguous.
  //
  // We always move a page to the front of the core list when we reference it
  // during b&#x2011;tree descent.

  // Link tier to the head of the most-recently used list.
  function link (head, entry) {
    var next = head.next;
    entry.next = next;
    next.previous = entry;
    head.next = entry;
    entry.previous = head;
    return entry;
  }

  // Unlink a tier from the most-recently used list.
  function _unlink (entry) {
    var next = entry.next, previous = entry.previous;
    next.previous = previous;
    previous.next = next;
    return entry;
  }

  // #### Cache Purge Trigger
  //
  // There are a few ways we could schedule a cache purge: elapsed time; after a
  // certain number of requests; when a reference count reaches zero; or when
  // when a limit is reached.
  //
  // We take the limits approach. The bulk of a cached page is the size of the
  // references array and the size of objects in records map. We keep track of
  // those sizes. When we reach an application developer-specified maximum size
  // for cached records and page references for the entire b&#x2011;tree, we
  // trigger a cache purge to bring it below the maximum size. The purge will
  // remove entries from the end of the most-recently used list until the limit
  // is met.
  //
  // #### JSON Size
  //
  // There is no way to get the size of a JavaScript object in memory, so we
  // don't have a way to know the actual size of cached pages in memory.
  //
  // We can get a pretty good relative measure of the size of a page, however,
  // using the length of the JSON strings used to store records and references.
  //
  // The ***JSON size*** of a branch page is the string length of the address
  // array when serialized to JSON, plus the string length of each key loaded
  // into memory when serialized to JSON. The JSON size of leaf page is the
  // string length of the file position array when serialized to JSON, plus the
  // string length of each record loaded in memory when serialized to JSON.
  //
  // This is not an exact measure of the system memory committed to the in
  // memory representation of the b&#x2011;tree. It is a fuzzy measure of the
  // relative heft of page in memory. An exact measure is not necessary. We only
  // need to be sure to trigger a cache purge at some point before we reach the
  // hard limits imposed by system memory or the V8 JavaScript engine.
  //

  // Adjust the JSON size of the given page and the entire b&#x2011;tree.
  function heft (page, s) {
    page.size += s;
    size += s;
  }

  // #### Pages Held for Housekeeping
  //
  // There may be page objects loaded for housekeeping only. When balancing the
  // tree, the length of a page is needed to determine if the page needs to be
  // split, or if it can merged with a sibling page.
  //
  // We only need the order of the page to create our balance plan, however, not
  // the cached references and records. The page object keeps a copy of the
  // order in a `order` property. We can delete the page's reference array, as
  // well as the page's object cache. The page object the page entry itself
  // cannot be removed from the cache until it is no longer needed to calculate
  // a split or merge.
  //
  // We use reference counting to determine if an entry is participating in
  // balance calculations. If the page is being referenced by a balancer, we
  // purge the reference array and the cached records and keys, but we do not
  // unlink the page object from the most-recently used list nor remove it from
  // the cache.

  // ### Leaf Pages
  //
  // Five key things to know about leaf pages:
  //
  // * A leaf page is an array of records.
  // * A leaf page cannot contain two records that share the same key; therefore,
  // the b&#x2011;tree cannot contain duplicates.
  // * The key of the first record is the key for the leaf page; the keys of all
  // other records in the leaf page are always greater than the key for the leaf
  // page.
  // * If the first record is deleted, we keep a it as a ghost record, for the
  // sake of the key, until the leaf page can be vacuumed.
  // * The leaf page file is a text file of JSON strings that is an append log
  // of record insertions and deletions.
  //
  // #### Constant Time
  //
  // In the abstract, a leaf page is an array of records.  Given an integer, the
  // leaf page will return the record stored at the offset of the array. This
  // lookup is performed in constant time when the record is in memory.
  //
  // This lookup is performed in more or less constant time when the record is
  // uncached, so long you're willing to say that random access into a file is
  // constant time for our purposes. Otherwise, lookup is *O(log n)*, where *n*
  // is the number of file system blocks used to store the leaf page file.
  //
  // #### Binary Search
  //
  // Our leaf page implementation maintains an array of file positions called a
  // positions array. A file position in the positions array references a record
  // in the leaf page file by its file position. The positions in the positions
  // array are sorted according to the b&#x2011;tree collation of the referenced
  // records.
  //
  // In the leaf page file, a record is stored as a JSON string. Not all of the
  // records are loaded when the page loads. Records that are not loaded when
  // the page is loaded are loaded as needed. The leaf page keeps a map (a
  // JavaScript `Object`) that maps file positions to deserialized records.
  //
  // Because the records are sorted, and because a lookup takes constant time,
  // we can search for a record in a leaf page using binary search in
  // logarithmic time.
  //
  // #### No Duplicates
  //
  // Leaf pages cannot contain duplicate records. Therefore, the b&#x2011;tree
  // cannot contain duplicate records.
  //
  // You can simulate duplicate records by adding a series value to your key
  // which is stored in your record. The cursor implementation is designed to
  // facilitate ***pseudo-duplicate*** keys in this fashion.
  //
  // In theory, leaf pages can contain `null`, and `null` can be used as a key.
  // However, if you want to allow `null` keys in your b&#x2011;tree, you almost
  // certainly want to allow more than one `null` key, so you'll end up using
  // the pseudo-duplicate strategy for `null` keys as well.
  //
  // #### Ghosts and Leaf Page Length
  //
  // When we delete the first record of a leaf page, we keep the first record
  // around, because its key value is the key value for the leaf page. Changing
  // the key of a leaf page requires re-balancing the tree, so we need to wait
  // until we balance to vacuum the deleted first record.
  //
  // When we delete the first record we increment the `ghosts` property of the
  // page by `1`. The acutal length of a leaf page is the value `length` less
  // the value of the `ghosts` property. Only the first record is ever turned
  // into a ghost if deleted, so the value of the `ghosts` property is only
  // ever `0` or `1`.
  //
  // #### Leaf Page Split
  //
  // If the length of a leaf page exceeds the leaf page order, the leaf page is
  // split when the b&#x2011;tree is balanced.

  // The in-memory representation of the leaf page includes the address of the
  // leaf page, the page address of the next leaf page, and a cache that maps
  // record file positions to records that have been loaded from the file.
  function createLeaf (override) {
    if (override.address == null) {
        while (!(nextAddress % 2)) nextAddress++;
        override.address = nextAddress++;
    }
    var page =
    { address: -1
    , cache: {}
    , entries: 0
    , locks: [[]]
    , ghosts: 0
    , positions: []
    , lengths: []
    , right: 0
    , size: 0
    };
    return extend(page, override);
  }

  constructors.leaf = createLeaf;

  // Add a leaf page or a branch page to the page cache and link it to the head
  // of the most-recently used list.
  function encache (page) {
    return cache[page.address] = link(mru, page);
  }

  // #### JSON Leaf Page Size
  //
  // The JSON size of a leaf page is the string length of the file position array
  // when serialized to JSON, plus the string length of each record loaded in
  // memory when serialized to JSON.
  //
  // ##### JSON Record Size
  //
  // We have to cache the calculated size of the record because we return the
  // records to the application developer. We're not strict about ownership, we
  // don't defensively copy the record before returning it or anything, so the
  // application developer may alter the record. When we uncache the record, we
  // won't be able to trust the recalculated JSON size. We keep a copy of the
  // size in an object in the leaf page cache.
  //
  // Each leaf page cache entry contains the record, key and the size of the
  // object at the time of caching.

  //
  function cacheRecord (page, position, record, key) {
    // Uncache the exisiting record.
    uncacheRecord(page, position);

    // Extract the key if none was provided.
    if (key == null) key = extractor(record);
    ok(key != null, "null keys are forbidden");

    // Create a cache entry.
    var entry = page.cache[position] = { record: record, key: key };

    // Calculate the size.
    entry.size = JSON.stringify(entry).length

    // Increment the page size and the size of the b&#x2011;tree.
    heft(page, entry.size);

    // Return our entry.
    return entry;
  }

  // Delete a record from the leaf page cache. Deduct the cached JSON size of the
  // record entry from the size of the page and the size of b&#x2011;tree.
  function uncacheRecord (page, position) {
    var entry;
    if (entry = page.cache[position]) {
      heft(page, -entry.size);
      delete page.cache[position];
    }
  }

  // ### Appends for Durability
  //
  // A leaf page file contains JSON objects, one object on each line. The
  // objects represent record insertions and deletions, so that the leaf page
  // file is essentially a log. Each time we write to the log, we open and close
  // the file, so that the operating system will flush our writes to disk. This
  // gives us durability.
  //
  // The caller determines which file should be written, so it opens and closes
  // the file descriptor. For record insertions and deletions, a file descriptor
  // is opened and closed for a single append. When rewriting an existing leaf
  // page in order to compact it, the file descriptor is kept open for the
  // multiple appends of the rewrite.
  //
  // The file descriptor must be open for append.

  function writeJSON (options, callback) {
    var check = validator(callback)
      , offset = 0
      , buffer
      , json
      , line
      , position
      , length
      ;

    // Read the file size if we have no insert position.
    if (options.page.position) positioned();
    else fs.fstat(options.fd, check(stat));

    // Get the file size as our insert position. Position to end of file.
    function stat (stat) {
      options.page.position = stat.size;
      positioned();
    }

    // Allocate a buffer and write the JSON and new line.
    function positioned () {
      // Update the position of last position array.
      if (options.type == "position") {
        options.page.bookmark = { position: options.page.position };
      }

      // Format the line with checksums.
      json = JSON.stringify(options.entry);
      line = json + " " + checksum(json);

      length = Buffer.byteLength(line, "utf8") + 1;

      buffer = new Buffer(length);
      buffer.write(line);
      buffer[length - 1] = 0x0A;

      if (options.type == "position") {
        options.page.bookmark.length = length;
      }

      position = options.page.position;

      send();
    }

    // Write may be interrupted by a signal, so we keep track of how many bytes
    // are actually written and write the difference if we come up short.
    // Return the file position of the appended JSON object.
    function send () {
      fs.write(options.fd, buffer, offset, buffer.length - offset, options.page.position, check(sent));
    }

    function sent(written) {
      options.page.position += written;
      offset += written;
      if (offset == buffer.length) {
        if (!(options.page.address % 2) || options.type == "footer") {
          callback(null, position, length);
        } else {
          writeFooter(options.fd, options.page, function () { callback(null, position, length) })
        }
      } else {
        send();
      }
    }
  }

  // ### Leaf Page Journal
  //
  // The leaf page acts as an edit journal recording edit events. Each event is
  // stored as a ***journal entry***. These journal entries are appended to the
  // leaf page files as JSON arrays, one JSON array per line in the file.
  //
  // There are three types of journal entires recorded in a leaf page: ***insert
  // entires***, ***delete entries***, and ***position array entries***.
  //
  // The insert and delete entries record changes to the the leaf page.
  // Beginning with an empty position array and reading from the start of the
  // file, the leaf tier is reconstituted by replaying the inserts and deletes
  // described by the insert and delete entries.
  //
  // Position array entries record the state of the position array later on in
  // the history of the leaf page file, so that we don't have to replay the
  // entire history of the leaf page file in order to load the leaf page.
  //
  // #### Position Array
  //
  // Each leaf page has a ***position array***. The position array references
  // the position in the leaf page file where an insert entry records the
  // insertion of a record into the leaf page. When we want the record, if it is
  // not in memory, then we read it from the leaf page file at the given file
  // position.
  //
  // When the record has been read from the leaf page file, it is cached in the
  // `cache` object property of the in&#x2011;memory page object indexed by its
  // file position.
  //
  // When we write an insert entry, we take note of the insert entries file
  // position in the leaf page and use that position as its place holder in the
  // position array.
  //
  // The position array maintains the file positions of the inert entries in the
  // collation order of the b&#x2011;tree.
  //
  // #### Per-Entry Housekeeping
  //
  // Each entry include a count of entries in the leaf page. The count of
  // entries is always increasing by one. It is essentially a line number. We
  // can detect missing lines by detecting a break in the series. We perform
  // this check when loading a leaf page, to an extent. We can perform it
  // against the entire leaf page if we suspect corruption. **TIDY**
  //
  // Each insert or delete entry also includes the count of records in the leaf
  // page including the effects of the entry itself. The position array entry
  // includes the count of records implicitly, since it includes the position
  // array, which contains a position for each entry.
  //
  // **TODO** Including or prior to. I almost like prior to better. Almost
  // easier to document. No, prior to is easier to document, but then it becomes
  // inconsistent with entry number.
  //
  // #### Insert Entries
  //
  // We determine if an entry is an insert entry by examining the first element
  // in the entry JSON array.
  //
  // If the first element is an integer greater than zero, it indicates an
  // insert entry. The integer is the one based index into the zero based
  // position array, indicating the index where the position of the current
  // insert object should be inserted. The next two elements are the journal
  // housekeeping. The last element is of the insert entry is the record object.
  //
  // The JSON array elements form a structure as follows.
  //
  //  * Count of entries in leaf page including insert.
  //  * One-based index into position array.
  //  * Count of records in leaf page including insert.
  //  * Record to insert.
  //
  // When we read the insert entry, we will place the record in the record cache
  // for the page, mapping the position to the record.

  // Write an insert object.
  function writeInsert (fd, page, index, record, callback) {
    var entry = [ ++page.entries, index + 1, record ];
    writeJSON({ fd: fd, page: page, entry: entry }, callback);
  }

  // #### Delete Entries
  //
  // If the first element of our entry is less than zero, it indicates a delete
  // entry. The absolute value of the integer is the one based index into the
  // zero based position array, indicating the index of the position array
  // element that should be deleted.
  //
  // The next two elements are the journal housekeeping. The last element of
  // the insert entry is the record object.
  //
  // The JSON array elements of a delete entry form a structure as follows.
  //
  //  * Count of entries in leaf page including delete.
  //  * Negated one-based index into position array.
  //  * Count of records in leaf page including delete.
  //
  // Special handling of a deleted first record is required when we replay the
  // journal. The first record of a leaf page is not actually deleted from their
  // in-memory pages, but ghosted. We keep them around because the key of the
  // first record is the key for a page.
  //
  // There is no special accounting necessary to record the fact that the first
  // record is a ghost in the delete entry. We can see that it was the first
  // record that was deleted.
  //
  // There are no other elements in the JSON array for a delete entry, just the
  // negated one&#x2011;based index of the record to delete.

  // Write a delete object.
  function writeDelete (fd, page, index, callback) {
    var entry = [ ++page.entries, -(index + 1) ];
    writeJSON({ fd: fd, page: page, entry: entry }, callback);
  }

  // Read or write buffers to the file system. We work with buffers, not
  // streams. For binary especially, we're mostly just filling buffers, so we
  // may as well perform our file operations on buffers.
  //
  // Note that both `fs.read` and `fs.write` can be interrupted by the
  // operating system, just like any system call. We need to check to see if
  // we've actually read or written the amount we expected, and continue with
  // the remainder if we haven't.

  //
  function io (direction, filename, callback) {
    var check = validator(callback);

    // looks like I'm getting ready to say goodbye to append "a".
    fs.open(filename, direction[0], check(opened));

    function opened (fd) {
      fs.fstat(fd, check(stat));

      function stat (stat) {
        callback(null, fd, stat, function (buffer, position, callback) {
          var check = validator(callback), offset = 0;

          var length = stat.size - position
          var slice = length < buffer.length ? buffer.slice(0, length) : buffer

          done(0);

          function done (count) {
            if (count < slice.length - offset) {
              offset += count;
              fs[direction](fd, slice, offset, slice.length - offset, position + offset, check(done));
            } else {
              callback(null, slice, position);
            }
          }
        })
      }
    }
  }

  // #### Position Array Entries
  //
  // A position array entry contains the position array itself. On occasion, we
  // store a copy of a constructed position array entry in the leaf page file so
  // that we can read a large leaf page file quickly.
  //
  // When we read a leaf page file, if we read from the back of the file toward
  // the front, we can read backward until we find a position array entry. Then
  // we can read forward to the end of the file, applying the inserts and
  // deletes that occurred after we wrote the position array entry.
  //
  // When a leaf page file is large, stashing the constructed position array at
  // the end means that the leaf page can be loaded quickly, because we will
  // only have to read backwards a few entries to find a mostly completed
  // position array. We can then read forward from the array to amend the
  // position array with the inserts and deletes that occurred after it was
  // written.
  //
  // Not all of the records will be loaded when we go backwards, but we have
  // their file position from the position array, so we can jump to them and
  // load them as we need them. That is, if we need them, because owing to
  // binary search, we might only need a few records out of a great many records
  // to find the record we're looking for.
  //
  // The position array entry includes some constant properties of the leaf
  // page.
  //
  // We write an array with a leaf page file format version number, indicating
  // the version of the leaf page file format, and therefore the version of
  // the entire b&#x2011;tree file format.
  //
  // We also include the address of the right sibling. This address will only
  // change when the leaf page file is rewritten.
  //
  // The JSON array elements of a delete entry form a structure as follows:
  //
  //  * Count of entries in leaf page including insert.
  //  * Zero to indicate a position array entry.
  //  * Leaf page file format version number.
  //  * Address of the right sibling leaf page.
  //  * Count of ghost records, only ever `0` or `1`.
  //  * The position array.
  //
  // The position array entry also acts as a header. We always place one at the
  // start of a leaf page, so that we can look at the head of the head of a leaf
  // page file to find its version and right sibling leaf page.
  //
  // **TK**: Counted b&#x2011;trees.

  // Write a position array entry.
  function writePositions (fd, page, callback) {
    var entry = [ ++page.entries, 0, 1, page.right, page.ghosts ]
    entry = entry.concat(page.positions).concat(page.lengths);
    writeJSON({ fd: fd, page: page, entry: entry, type: "position" }, callback);
  }

  function writeFooter (fd, page, callback) {
    ok(page.address % 2 && page.bookmark != null);
    var entry = [
      ++page.entries, 0, 0, page.bookmark.position, page.bookmark.length,
      page.right || 0, page.positions.length - page.ghosts
    ];
    writeJSON({ fd: fd, page: page, entry: entry, type: "footer" }, callback);
  }

  // #### Reading Leaves

  // Here is the backward search for a position in array in practice. We don't
  // really ever start from the beginning. The backwards-then-forwards read is
  // just as resilient.

  //
  function readLeaf (page, callback) {
    // Note that if we don't find a position array that has been written to the
    // leaf page file, then we'll start with an empty position array.
    var positions = []
      , lengths = []
      , bookmark
      , check = validator(callback)
      ;

    // We don't cache file descriptors after the leaf page file read. We will
    // close the file descriptors before the function returns.
    io('read', filename(page.address), check(opened))

    function opened (fd, stat, read) {
      var buffer = new Buffer(options.readLeafStartLength || 1024);
      read(buffer, Math.max(0, stat.size - buffer.length), check(footer));

      // todo: check that the last character is a new line.
      function footer (slice) {
        for (var i = slice.length - 1; i != -1; i--) {
          if (slice[i] == 0x5b) {
            var footer = readLine(slice.toString('utf8', i))
            bookmark = { position: footer[3], length: footer[4] };
            read(new Buffer(bookmark.length), bookmark.position, check(positioned));
            return;
          }
        }
        // We're probably going to request a medic, not try to heal on open.
        throw new Error('cannot find footer in last ' + buffer.length + ' bytes')
      }

      function positioned (slice) {
        var positions = readLine(slice.toString('utf8', 0, bookmark.length));

        page.entries = positions.shift();
        ok(positions.shift() == 0, "expected housekeeping type");
        ok(positions.shift() == 1, "expected position type");
        page.right = positions.shift();
        page.ghosts = positions.shift();

        ok(!(positions.length % 2), "expecting even number of positions and lengths");
        var lengths = positions.splice(positions.length / 2)

        // Prime our page with the positions array read from the leaf file, or else
        // an empty positions array.
        splice('positions', page, 0, 0, positions);
        splice('lengths', page, 0, 0, lengths);

        page.bookmark = bookmark;

        replay(fd, stat, read, page, bookmark.position + bookmark.length, check(designate));
      }
    }

    // Unless this is the leftmost leaf page, load the first record and assign
    // its key as the page's key.
    function designate () {
      if (page.address == 1) {
        callback(null, page);
      } else {
        stash(page, 0, check(designated));
      }
    }

    function designated (entry) {
      page.key = entry.key
      callback(null, page)
    }
  }

  // Replay a leaf page or branch page log into the `page` starting with the
  // entry at the given `position`. The `fd`, `stat`, and `read` parameters are
  // the results of calling `io` to create a read function. This is done by the
  // caller.
  //
  // The `replay` function accepts the `io` function properties so that we don't
  // have to close and reopen the file when it is called from `readLeaf`. In the
  // case of a leaf page, the `readLeaf` will have opened the page log file
  // to find a positions array waypoint.

  //
  function replay (fd, stat, read, page, position, callback) {
    var check = validator(callback)
      , leaf = !!(page.address % 2)
      , buffer = new Buffer(options.readLeafStartLength || 1024);

    read(buffer, position, check(replay));

    // Each line is terminated by a newline. In case you're concerned that the
    // scan for a new line will mistake a byte inside a multi-byte character for
    // a newline, have a look at
    // [UTF-8](http://en.wikipedia.org/wiki/UTF-8#Description). All bytes
    // participating in a multi-byte character have their leading bit set, all
    // single-byte characters have their leading bit unset. Therefore, `"\n"` is
    // unambiguous.
    function replay (slice, start) {
      var stop = slice.length, offset = 0;
      for (var i = offset, I = slice.length; i < I; i++) {
        if (slice[i] == 0x0a) {
          var position = start + offset;
          var length = (i - offset) + 1;
          ok(length);
          var entry = readLine(slice.toString('utf8', offset, offset + length));
          ok(entry.shift() == ++page.entries, "entry count is off");
          var index = entry.shift();
          if (leaf) {
            if (index > 0) {
              splice('positions', page, index - 1, 0, position);
              splice('lengths', page, index - 1, 0, length);
              cacheRecord(page, position, entry.pop());
            } else if (~index == 0 && page.address != 1) {
              ok(!page.ghosts, "double ghosts");
              page.ghosts++;
            } else if (index < 0) {
              uncacheRecord(page, splice('positions', page, -(index + 1), 1).shift());
              splice('lengths', page, -(index + 1), 1);
            }
          } else {
            if (index > 0) {
              splice('addresses', page, index - 1, 0, entry.shift());
            } else {
              splice('addresses', page, ~index, 1);
            }
          }
          offset += length;
        }
      }

      if (start + buffer.length < stat.size) {
        if (offset == 0) {
          buffer = new Buffer(buffer.length * 2)
          read(buffer, start, check(replay));
        } else {
          read(buffer, start + offset, check(replay));
        }
      } else {
        fs.close(fd, check(closed));
      }
    }

    // Return the loaded page.
    function closed () {
      callback(null, page);
    }
  }

  // We can load a leaf page from position array that has been written to the
  // leaf page file through to the end of the file loading the records appended
  // since the position array was logged. We can load the appended records
  // without having to load all of the records referenced by the position array
  // prior to its logging. We load the prior records as they are requested.
  //
  // To load a record, we open the file and jump to the position indicated by
  // the position array. We allocate a buffer using the length for the record
  // that was stored in the positions entry. We then read the insert object that
  // introduced the record to the leaf page file.
  //
  // We open a file descriptor and then close it after the record has been read.
  // The desire to cache the file descriptor is strong, but it would complicate
  // the shutdown of the b&#x2011;tree. As it stands, we can always simply let
  // the b&#x2011;tree succumb to the garbage collector, because we hold no
  // other system resources that need to be explicitly released.

  //
  function readRecord (page, position, length, callback) {
    var check = validator(callback), record;

    if (page.position) positioned();
    else fs.stat(filename(page.address), check(stat));

    function stat (stat) {
      page.position = stat.size;
      positioned(stat.size)
    }

    function positioned () {
      fs.open(filename(page.address), "r", check(input));
    }

    function input (fd) {
      read();

      // todo: retry if not all read.
      function read () {
        fs.read(fd, new Buffer(length), 0, length, position, check(json));
      }

      function json (bytes, buffer) {
        ok(bytes == length, "incomplete read");
        ok(buffer[length - 1] == 0x0A, "newline expected");
        record = readLine(buffer.toString("utf8")).pop();
        fs.close(fd, check(closed));
      }
    }

    function closed() {
      callback(null, record);
    }
  }

  // Over time, a leaf page file can grow fat with deleted records. Each deleted
  // record means there's an insert object that is no longer useful. The delete
  // record is only useful as a marker. We vacuum the leaf page file to get rid
  // of these pairs of objects that have negated each other.
  //
  // We vacuum a leaf page file by writing it to a replacement leaf page file,
  // then using `relink` to replace the current leaf page file with the
  // replacement.
  //
  // All records referenced by the current position array are appended into
  // the replacement leaf page file using insert objects. A position array
  // object is appended to the end of the replacement leaf page file. The
  // rewritten leaf page file will load quickly, because the position array
  // object will be found immediately.

  // Note that we close the file descriptor before this function returns.
  function rewriteLeaf (page, suffix, callback) {
    var check = validator(callback)
      , cache = {}
      , index = 0
      , fd, positions, lengths
      ;

    // Open the new leaf page file and reset our file position.
    fs.open(filename(page.address, suffix), "a", 0644, check(opened));

    function opened ($1) {
      fd = $1;

      page.position = 0;
      page.entries = 0;

      // Capture the positions, while truncating the page position array.
      positions = splice('positions', page, 0, page.positions.length);
      lengths = splice('lengths', page, 0, page.lengths.length);

      // Write an empty positions array to act as a header.
      writePositions(fd, page, check(iterate))
    }

    function iterate () {
      if (positions.length) rewrite();
      else if (page.positions.length) append();
      else close();
    }

    // Rewrite an object in the positions array.
    function rewrite () {
      var position = positions.shift(), length = lengths.shift(), object;

      // Read the object from the current page.
      stash(page, position, length, check(stashed));

      // Uncache the object and write the record to the new file.
      function stashed ($) {
        uncacheRecord(page, position);
        writeInsert(fd, page, index++, (object = $).record, check(written));
      }

      // Append the position to the page and stash the position and object.
      function written (position, length) {
        splice('positions', page, page.positions.length, 0, position);
        splice('lengths', page, page.lengths.length, 0, length);
        cache[position] = object;
        iterate();
      }
    }

    // Cache the objects we've read from the existing page and write a positions
    // array entry.
    function append() {
      var object;
      for (var position in cache) {
        object = cache[position];
        cacheRecord(page, position, object.record, object.key);
      }
      writePositions(fd, page, check(close));
    }

    // Close our file.
    function close() {
      fs.close(fd, callback);
    }
  }

  // ### Branch Pages
  //
  // Five key things to know about branch pages.
  //
  // * A branch page contains an array of addresses of child pages.
  // * The left most address is the left child of the entire branch page.
  // * The branch page keys are looked up as needed by descending the tree to
  // the left most leaf page of a child and using the leaf page key.
  // * The root branch page is always at address `0`.
  //
  // To find a record in the b&#x2011;tree, we first use a tree of branch pages
  // to find the leaf page that contains our record.
  //
  // A branch page contains the addresses of ***child pages***. This array of
  // page addresses is essentially an *array of children*.
  //
  // The child addresses are ordered according to the b&#x2011;tree collation of
  // the keys of the directly or indirectly referenced leaf pages.
  //
  // There are three types of branch pages, penultimate branch pages, interior
  // branch pages, and the root branch page.
  //
  // #### Penultimate Branch Pages
  //
  // A penultimate branch page is a branch page whose children are leaf pages.
  // If a branch page is not penultimate, then its children are branch pages.
  //
  // In a penultimate branch page, the array of children is ordered by the
  // b&#x2011;tree collation using a first record in the referenced leaf page
  // for ordering.  That is, the first record of the leaf page is used as the
  // key associated with a page address in a penultimate branch page.
  //
  // The non-leaf nodes of a b&#x2011;tree have the property that the number of
  // node children is one greater than the number of keys. We obtain this
  // property by treating the first child as the left child of the entire page,
  // and excluding its key from the search. We search the subsequent keys to
  // find the first key that is grater than or equal to the record sought. If we
  // encounter a key that is less than all the keys in the branch page, we know
  // that the record is contained in the leaf page child associated with the key
  // before it. We are able to perform this search using binary search in
  // logarithmic time.
  //
  // By ignoring the key of the first leaf page, the penultimate branch page has
  // a number of children that is one greater than the number of keys.
  //
  // **TK**: Not really explaining that it's only the left most leaf page that
  // is a special case. Suppose that I'm trying to introduce the concept.
  //
  // Notice that, when we are inserting a record into a leaf page other than the
  // left leaf page, we add it to a leaf page whose key is equal to or greater
  // than the penultimate branch key, so that the first record does not change,
  // and therefore that penultimate branch key does not change. The exception is
  // the left most leaf page, which accepts all the records less than the first
  // key, and therefore may accept a record less than its current least record.
  //
  // An insertion can only insert into the left most leaf page of a
  // penultimate branch page a record less than the least record of the leaf
  // page.
  //
  // #### Interior Branch Pages
  //
  // A branch page whose children are other branch pages is called an interior
  // branch page.
  //
  // Like the penultimate branch page, we treat the first child of an interior
  // branch page as the left child of the entire page. Like the penultimate
  // branch page the subsequent children have an associated key that is the
  // first record of a leaf page.
  //
  // The key is obtained by descending the sub&#x2011;tree referenced by the
  // child. We first visit the branch page referenced by the child. We then
  // visit left children recursively, visiting the left child of the child, and
  // the left child of any subsequently visited children, until we reach a leaf
  // page. The first record of that leaf page is the key to associate with the
  // child address in the address array of the interior branch page.
  //
  // It is the nature of the b&#x2011;tree that keys move up to the higher
  // levels of the tree as pages split, while preserving the collation order of
  // the keys. When a branch page splits, a key from the middle of the page is
  // chosen as a partition. The partition is used as the key for the right half
  // of the split page in the parent page.
  //
  // Our implementation does not store the keys, as you may have noticed, but
  // descends down to the leaf page to fetch the record to use as a key.
  //
  // We start from a penultimate page as a root page. When a leaf page fills, we
  // split it, creating a new right leaf page. The penultimate page uses the
  // first record of the new right page as the key to associate with that page.
  //
  // When the root penultimate page is full we split it, so that the root page
  // is an interior page with two children, which are two penultimate pages. The
  // tree now contains a root interior branch page, with a left penultimate
  // branch page and a right penultimate branch page.
  //
  // The root interior branch page has one key. Prior to split, that key was
  // associated with the address of a child leaf page. After split, the key is
  // associated with the right penultimate branch page. The leaf page is now the
  // left child of the right penultimate branch page.
  //
  // When we visit the root interior page, to obtain the key to associate with
  // the right penultimate page, we visit the right penultimate page, then we
  // visit its left child, the leaf page whose first record is the key.
  //
  // #### Root Branch Page
  //
  // The root page is the first page we consult to find the desired leaf page.
  // Our b&#x2011;tree always contains a root page. The b&#x2011;tree is never
  // so empty that the root page disappears. The root page always has the same
  // address.
  //
  // **TK**: move. Until the root branch page is split, it is both the root
  // branch page and a penultimate branch page.
  //
  // ### Keys and First Records
  //
  // We said that it is only possible for an insertion to insert into the left
  // most child leaf page of a penultimate branch page a record less than the
  // least record. We can say about a tree rooted by an interior branch page,
  // that an insertion is only able to insert into the left most leaf page in
  // the *entire tree* a record less than the least record.
  //
  // **TK**: Confusing.
  //
  // Using our example tree with one root interior page, with two penultimate
  // branch page children, we cannot insert a record into the right penultimate
  // branch page that will displace the first record of its left most child
  // branch, because that first record is the key for the right penultimate
  // branch page. When we insert a record that is less than the key, the search
  // for a leaf page to store the record goes to the left of the key. It cannot
  // descend into the right penultimate branch page, so it is impossible for it
  // be inserted into left child of the right penultimate branch page, so the
  // first record left child of the penultimate branch page will never be
  // displaced by an insertion.
  //
  // Only if we insert a record that is less than least key of the left
  // penultimate page do we face the possibility of displacing the first record
  // of a leaf page, and that leaf page is the left most leaf page in the entire
  // tree.
  //
  // This maintains a property of the b&#x2011;tree that for every leaf page
  // except the left most leaf page, there exists a unique branch page key
  // derived from the first record of the page.
  //
  // As above, you can find the first record used to derive a key by visting the
  // child and going left. You can find the leaf page to the left of the leaf
  // page used to derive a page branch key, by visiting the child to the left of
  // the key and going right.
  //
  // **NOTE**: Literate programming has finally materialized with Docco and
  // CoffeeScript.
  //
  // When the root page splits, it becomes an interior branch page. Until it
  // splits it is both the root page and a penultimate page.

  // ### Branch Page Files
  //
  // We create new branch pages in memory. They do not exist on disk until
  // they are first written.
  //
  // A new branch page is given the next unused page number.
  //
  // In memory, a branch page is an array of child page addresses. It keeps
  // track of its key and whether or not it is a penultimate branch page. The
  // cache is used to cache the keys associated with the child page addresses.
  // The cache maps the address of a child page to a key extracted from the
  // first record of the leaf page referenced by the child page address.
  //
  // Our in-memory branch page is also cached and added as a node an MRU list.
  // We must make sure that each page has only one in memory representation,
  // because the in memory page is used for locking.

  //
  function createBranch (override) {
    if (override.address == null) {
        while (nextAddress % 2) nextAddress++;
        override.address = nextAddress++;
    }
    var page =
    { address: -1
    , addresses: []
    , cache: {}
    , locks: [[]]
    , penultimate: true
    , entries: 0
    , size: 0
    };
    return extend(page, override);
  }
  constructors.branch = createBranch;

  // #### Branch Page JSON Size

  // The branch page JSON size is JSON string length of the address array, plus
  // the JSON string length of each cached key.
  //
  // ##### JSON Reference Array Size
  //
  // The `splice` method adds or remove references from the reference array
  // using the semantics similar to JavaScript's `Array.splice`. Similar, but
  // not identical. The replacement values are passed in as an array, not as
  // arguments at the end of a variable argument list.
  //
  // This wrapper for a basic array operation exists for the sake of the JSON
  // size adjustments, which would otherwise be scattered around the code. It is
  // the only place where the JSON string length of the reference array is
  // accounted for.

  //
  function splice (collection, page, offset, length, insert) {
    ok(typeof collection == 'string', 'incorrect collection passed to splice');

    // Get the references, either page addresses or record positions.
    var values = page[collection]
      , json
      , removals
      ;

    ok(values, 'incorrect collection passed to splice');

    // We remove first, then append. We used the array returned by `splice` to
    // generate a JSON substring, whose length we remove form the JSON size of
    // the page. We also decrement the page length.
    if (length) {
      removals = values.splice(offset, length);

      json = values.length == 0 ? "[" + removals.join(",") + "]"
                                : "," + removals.join(",");

      heft(page, -json.length);
    } else {
      removals = [];
    }

    // Insert references.
    if (insert != null) {
      // Convert a single argument into an array.
      if (! Array.isArray(insert)) insert = [ insert ];
      // First we generate a JSON substring from the insert array, whose length
      // we add to the JSON size of the page. We also increment the page length.
      if (insert.length) {
        json = values.length == 0 ? "[" + insert.join(",") + "]"
                                  : "," + insert.join(",");

        heft(page, json.length);

        values.splice.apply(values, [ offset, 0 ].concat(insert));
      }
    }
    // Return the removed references.
    return removals;
  }

  // ##### JSON Key Size

  // Add a key to the branch page cache and recalculate JSON size. Uncache any
  // existing key for the address.
  function cacheKey (page, address, key) {
    uncacheKey(page, address);
    heft(page, JSON.stringify(key).length);
    page.cache[address] = key;
  }

  // Remove a key from the branch page cache if one exists for the address.
  // Deduct the JSON string length of the key from the JSON size.
  //
  // Often times you return to have a look at this function to see why it wants
  // an address and not an index. Before you go breaking things, have a look
  // around. You'll find places where you've spliced the reference array and
  // you're working with the removed addresses. In this case the index is lost.
  function uncacheKey (page, address) {
    if (page.cache[address] !== (void(0))) {
      heft(page, -JSON.stringify(page.cache[address]).length);
      delete page.cache[address];
    }
  }

  // We write the branch page as a log of key inserts and deletes. We use the
  // same format as the leaf page where instead of a record, we write just the
  // key. We also add of a page address to a branch page insert entry.
  //
  // TODO: At this point, we're not actually using the keys. That is a few
  // commits away. The next statment will change too.
  //
  // TODO: Change: We always write a page branch first to a replacement file,
  // then move it into place using `replace`.

  //
  function writeBranch (page, suffix, callback) {
    var check = validator(callback)
      , addresses = page.addresses.slice();

    page.entries = 0;
    page.position = 0;

    fs.open(filename(page.address, suffix), "a", 0644, check(opened));

    function opened (fd) {
      write();

      function write () {
        if (addresses.length) {
          var address = addresses.shift();
          page.entries++;
          var entry = [ page.entries, page.entries, address ];
          writeJSON({ fd: fd, page: page, entry: entry }, check(write));
        } else {
          fs.close(fd, check(closed));
        }
      }
    }

    function closed () {
      callback(null);
    }
  }

  // To read a branch we read it as a log file of key inserts and deletes. The
  // `replay` method will determine the type of page &mdash; branch page or leaf
  // page &mdash; and update the appropriate collection; addresses for branch
  // pages, positions and lengths for leaf pages.

  //
  function readBranch (page, callback) {
    var check = validator(callback);
    io('read', filename(page.address), check(opened));

    function opened (fd, stat, read) {
      replay(fd, stat, read, page, 0, check(loadKeys));

      function loadKeys (page) {
        var index = 1;

        loadKey();

        function loadKey () {
          if (index < page.addresses.length) {
            designate(page, index++, check(loadKey));
          } else {
            callback(null, page);
          }
        }
      }
    }
  }

  // ### B-Tree Initialization
  //
  // After creating a `Strata` object, the client will either open the existing
  // database, or create a new database.
  //
  // #### Creation
  //
  // Creating a new database will not create the database directory. The
  // database directory must already exist. It must be empty. We don't want to
  // surprise the application developer by blithely obliterating an existing
  // database.
  //
  // The initial database has a single root penultimate branch page with only a
  // left child and no keys. The left child is a single leaf page that is empty.
  //
  // Note that the address of the root branch page is `0` and the address of the
  // left most leaf page is `-1`. This will not change. Even as the
  // b&#x2011;tree is balanced with splits and mergers of leaf pages, the root
  // branch page is always `0` and left most leaf page is always `-1`.

  //
  function create (callback) {
    var root, leaf, check = validator(callback), count = 0;

    stat();

    function stat () {
      fs.stat(directory, check(extant));
    }

    function extant (stat) {
      ok(stat.isDirectory(), "database " + directory + " is not a directory.");
      fs.readdir(directory, check(empty));
    }

    function empty (files) {
      ok(!files.filter(function (f) { return ! /^\./.test(f) }).length,
         "database " + directory + " is not empty.");

      // Create a root branch with a single empty leaf.
      root = encache(createBranch({ penultimate: true }));
      leaf = encache(createLeaf({}));
      splice('addresses', root, 0, 0, leaf.address);

      // Write the root and leaf branches.
      writeBranch(root, ".replace", check(written));
      rewriteLeaf(leaf, ".replace", check(written));
    }

    // When we've written the leaves, move them into place.
    function written () {
      if (++count == 2) {
        replace(leaf, ".replace", check(replaced));
        replace(root, ".replace", check(replaced));
      }
    }

    function replaced() {
      if (--count == 0) toUserLand(callback);
    }
  }

  // #### Opening
  //
  // Opening an existing database is a matter of checking for any evidence of a
  // hard shutdown. You never know. There may be a banged up leaf page file, one
  // whose last append did not complete. We won't know that until we open it.
  //
  // **TODO**: Ah, no. Let's revisit. Here's a simple strategy. Open touches a
  // file.  Closing deletes the file. If we open and the file exists, then we
  // probably have to inspect every file that was modified after the
  // modification, adjusting for dst? No because we'll be using seconds since
  // the epoch. Only if the system time is changed do we have a problem.
  //
  // Thus, we have a reference point. Any file after needs to be inspected. We
  // load it, and our `readLeaf` function will check for bad JSON, finding it
  // very quickly.
  //
  // Now, we might have been in the middle of a split. The presence of `*.new`
  // files would indicate that. We can probably delete the split. Hmm..
  //
  // We can add more to the suffix. `*.new.0`, or `*.commit`, which is the last
  // replacement. If we have a case where there is a file named `*.commit` that
  // does not have a corresponding permanent file, then we have a case where the
  // permanent file has been deleted and not linked, but all the others have
  // been, since this operation will go last, so we complete it to go forward.
  //
  // Otherwise, we delete the `*.commit`. We delete all the replacements that
  // are not commits.
  //
  // We can always do a thorough rebuild of some kind.
  //
  // Probably need "r" to not create the crash file, in case we're reading from
  // a read only file system, or something.

  // &mdash;
  function open (callback) {
    var check = validator(callback);

    fs.stat(directory, check(stat));

    function stat (error, stat) {
      fs.readdir(directory, check(list));
    }

    function list (files) {
      files.forEach(function (file) {
        if (/^\d+$/.test(file)) {
          nextAddress = Math.max(+(file) + 1, nextAddress);
        }
      });
      toUserLand(callback, null);
    }
  }

  // We close after every write, so there are no open file handles.
  //
  // **TODO**: Need to actually purge cache and set sizes to zero.

  // &mdash;
  function close (callback) {
    // In case this is the last we hear from the user.
    thrownByUser = null;

    // Nothing to do now, since we're always writing our pages. Eventually we're
    // going to flush dirty pages or assert that there are no dirty pages.
    toUserLand(callback, null);
  }

  // **TODO**: Close medic file.

  // ### Concurrency
  //
  // Although there is only a single thread in a Node.js process, the
  // b&#x2011;tree is still a concurrent data structure.
  //
  // When we navigate the tree from the root to search for a key in the tree, we
  // descend the b&x2011;. The act of descending the b&#x2011;tree is called a
  // ***descent***.
  //
  // A descent is a unit of work in our concurrency model. One descent of the
  // b&#x2011;tree can make progress while another descent of the tree is
  // waiting on evented I/O. Instead of thinking about concurrency in terms of
  // threads we talk about concurrent descents of the b&#x2011;tree.
  //
  // Descents of the b&#x2011;tree can become concurrent when descent encounters
  // a page that is not in memory. While it is waiting on evented I/O to load
  // the page files, the main thread of the process can make progress on another
  // request to search or alter the b&#x2011;tree, it can make process on
  // another descent.
  //
  // This concurrency keeps the CPU and I/O loaded.
  //
  // Note that the b&#x2011;tree must only be read and written by a single
  // Node.js process. You can't have two Node.js processes serving the same
  // database directory.
  //
  // ### Locking
  //
  // Every page has a read/write lock. Pages are obtained through the `IO.lock`
  // method, so that every time we obtain a page, it comes to us locked.
  //
  // Locking is internal and not exposed to the application developer. However,
  // the application developer must remember to close cursors, in order to
  // release the locks that cursors hold.
  //
  // #### Shared and Exclusive
  //
  // When we obtain a lock to read the page, we obtain a ***shared lock***. The
  // shared lock allows other descents to also obtain a lock on the page. With a
  // shared lock, you can read the page, knowing that it will not change.
  //
  // When we obtain a lock to write the page, we obtain an ***exclusive lock***.
  // The exclusive lock prevents all other descents from obtaining any sort of
  // lock on the page. No other page will be able to obtain neither a shared nor
  // an exclusive lock.
  //
  // #### Locking Sub-Trees
  //
  // When we descend the tree, moving from parent page to child page, we obtain
  // locks in descending order. This means that if we obtain an exclusive lock
  // on a page, no other descent will be able to travel from the parent of that
  // page, to the children of that page. It effectively blocks new descents from
  // entering the ***sub&#x2011;tree*** that is defined by the exclusively
  // locked page and all its children.
  //
  // #### Rationale for Locking
  //
  // Locking prevents race conditions where a descent that is waiting on an
  // evented I/O request returns to find that the structure of the b&#x2011;tree
  // has changed drastically. For example, while a descent was waiting for a
  // leaf page to load into memory, another descent might load the same page
  // into memory, merge the leaf page with its sibling, and delete it.
  //
  // #### Caching
  //
  // Locks prevent cache purges. When a cache purge is triggered, a page with
  // outstanding locks will not be purged from the cache.
  //
  // #### Lock Properties
  //
  // The locking mechanism is a writer-preferred shared read, exclusive write
  // lock. If a descent holds an exclusive write lock, then all lock requests by
  // other descents are blocked. If one or more descents hold a shared read
  // lock, then any request for an exclusive write lock is blocked. Any request
  // for a shared read lock is granted, unless an exclusive write lock is
  // queued.
  //
  // The locks are not re-entrant.

  // &mdash;
  function lock (address, exclusive, callback) {
    // We must make sure that we have one and only one page object to represent
    // the page. The page object will maintain the lock queue for the page. It
    // won't do to have different descents consulting different lock queues.
    // There can be only one.
    //
    // The queue is implemented using an array of arrays. Shared locks are
    // grouped inside one of the arrays in the queue element. Exclusive locks
    // are queued alone as a single element in the array in the queue element.
    var page, creator, locks;

    //
    if (page = cache[address]) {
      // Move the page to the head of the most-recently used list.
      link(mru, _unlink(page));
    //
    } else {
      // Create a page to load with an empty `load` queue. The presence of the
      // `load` queue indicates that the page needs to be read from file.
      page = encache(constructors[address % 2 ? "leaf" : "branch"]({ address: address, load: [] }));
    }

    // #### Lock Implementation
    //
    // We don't use mutexes, of course, because Node.js doesn't have the concept
    // of mutexes to protect critical sections of code the way that threaded
    // programming platforms do.
    //
    // Nor do we use file system locking.
    //
    // Instead, we simulate locks using callbacks. A call to `lock` is an
    // evented function call that provides a callback. If the `lock` method can
    // grant the lock request to the caller, the lock method will invoke the
    // callback.
    //
    // If the `lock` method cannot grant the lock request, the `lock` method
    // will queue the callback into a queue of callbacks associated with the
    // page. When other descents release the locks that prevent the lock
    // request, the lock request callback is dequeued, and the callback invoked.

    // The callback is always added to the queue, even if it is not blocked and
    // will execute immediately. The array in the queue element acts as a lock
    // count.
    //
    // If the callback we add to the queue is added to the first queue element,
    // then it is executed immediately. The first queue element is the active
    // queue element. Otherwise, it will be executed when the queue elements
    // before it have completed.
    //
    // When an exclusive lock is queued, an empty array is appended to the
    // queue. Subsequent read lock callbacks are appended to the array in the
    // last element. This gives exclusive lock callbacks priority.

    // If there were no locks outstanding, then it could be the case that page
    // has not been loaded. Note that, if there were locks outstanding, then the
    // page is already loaded, so we do not have to invoke load now or later.
    locks = page.locks
    if (exclusive) {
      locks.push([ callback ]);
      locks.push([]);
      if (locks[0].length == 0) {
        locks.shift()
        load(page, callback);
      }
    } else {
      locks[locks.length - 1].push(callback);
      if (locks.length == 1) {
        load(page, callback);
      }
    }
  }

  // #### Check JSON Size

  // Here's a temporary block of code that will assert that we're keeping an
  // accurate count of the heft of our pages. After a while, this code will be
  // removed, and we'll count on assertions in our unit tests to catch errors.

  // This only checks that the JSON size is correct for the give page contents,
  // not for the entire b&#x2011;tree.
  //
  // TODO Throw? Who's catching these?
  function checkJSONSize (page) {
    var size = 0, position, object;
    if (page.address % 2) {
      if (page.positions.length) {
        size += JSON.stringify(page.positions).length
        size += JSON.stringify(page.lengths).length
      }
      for (position in page.cache) {
        object = page.cache[position];
        size += object.size
      }
    } else {
      if (page.addresses.length) {
        size += JSON.stringify(page.addresses).length
      }
      for (position in page.cache) {
        object = page.cache[position];
        size += JSON.stringify(object).length
      }
    }
    ok(size == page.size, "sizes are wrong");
  }

  // ### Load

  // One or more descents may encounter the same unloaded page. Only one descent
  // should load it, the others should wait.
  //
  // In `load` we ensure that only the first descent will actually invoke the
  // load function for the page. If we are the first to encounter an unloaded
  // page, we push our callback onto the `load` queue of the page, and invoke
  // the correct read function for the page time. We provide a callback that
  // will invoke all the queued callbacks in the `load` queue to the read
  // function.
  //
  // If we encounter an unloaded page, but there are already callbacks in the
  // queue, we know that the first descent through has invoked read, and that
  // our callback will be invoked if we will simply place it in the `load` queue
  // and do nothing more.

  // &mdash;
  function load (page, callback) {
    // If the page is not loaded, load it.
    if (page.load) {
      // Add our callback to the list of waiting callback.
      page.load.push(callback);
      // If we are the first one through, create a group callback function, then
      // pass it to the load function for the page type.
      if (page.load.length == 1) {
        // Create a callback that will invoke all the callbacks queued to wait
        // for the page to load.
        //
        // TODO Errors going to multiple callbacks, invocations that can
        // generate multiple errors.
        function loaded (error) {
          var load = page.load.slice();
          // On error we reset the load list.
          if (error) {
            page.load.length = 0;
          // Otherwise, we delete the load list, because no load list means the
          // page is loaded.
          } else {
            checkJSONSize(page);
            delete page.load
          }
          load.forEach(function (callback) {
            unwind(callback, error, page);
          });
        }
        // Invoke the correct read function for the page type.
        if (page.address % 2) {
          readLeaf(page, loaded);
        } else {
          readBranch(page, loaded);
        }
      }
    // If the page is already loaded, we wave the descent on through.
    } else {
      //
      checkJSONSize(page);
      callback(null, page);
    }
  }

  // #### Unlock

  // When we release a lock, we simply shift a callback off of the array in the
  // first element of the queue to decrement the lock count. We are only
  // interested in the count, so it doesn't matter if the callback shifted by
  // the descent is the one that it queued.

  //
  function unlock (page) {
    // Note that it is not possible for this method to be called on any page that
    // has not already been loaded.
    checkJSONSize(page);
    var locks = page.locks;
    locks[0].shift()
    // Stop when we find a lock queue that has outstanding descents, or when we
    // reach the final queue of shared locks.
    while (locks[0].length == 0 && locks.length != 1) {
      locks.shift()
      // Each callback is scheduled using next tick. If any callback waits on
      // I/O, then another one will resume. Concurrency.
      locks[0].slice().forEach(function (callback) {
        unwind(callback, null, page);
      });
    }
  }

  // We're going to shadow `unlock` in the `Cursor` class, so keep a copy of
  // `Strata` version.
  //
  // **Note**: Wow! I just had a heck of a time debugging an update to `Cursor`
  // that called `unlock(rightLeafPage)`. Set me back trying to figure out why
  // it wasn't unlocking the page you passed it.

  //
  var _unlock = unlock;

  // Read a record cache entry from the cache. Load the record and cache it of
  // it is not already cached.
  function stash (page, positionOrIndex, length, callback) {
    var position = positionOrIndex;
    if (arguments.length == 3) {
      callback = length;
      position = page.positions[positionOrIndex];
      length = page.lengths[positionOrIndex];
    }
    ok(length);
    var stash;
    if (!(stash = page.cache[position])) {
      readRecord(page, position, length, validate(callback, function (record) {
        callback(null, cacheRecord(page, position, record));
      }));
    } else {
      callback(null, stash);
    }
  }

  // A note on the theoretical `null` key. If the collation order places `null`
  // before all other values, that's a good choice, because that means that it
  // will never be used for a branch key. If it is used as a branch key, the
  // branch will never be able to cache the key value, it will always have to
  // look it up, because its cache entry for the key will be `null`.
  //
  // But, don't use a `null` key. Create a pseudo-duplicate `null` instead.

  // Get the key for the record in the case of a leaf page, or the key of the
  // branch child page in the case of a branch page. Because this method
  // operates on both branch pages and leaf pages, our binary search operates on
  // both branch pages and leaf pages.
  function designate (page, index, callback) {
    var key;
    if (page.address % 2) {
      stash(page, index, validate(callback, function (entry) {
        callback(null, entry.key);
      }));
    } else if ((key = page.cache[page.addresses[index]]) === (void(0))) {
      var iter = page
        , iterIndex = index
        , stack = []
        ;

      next();

      function next () {
        var key;
        if (!(iter.address % 2)) {
          lock(iter.addresses[iterIndex], false, validate(callback, function (locked) {
            iterIndex = 0;
            stack.push(iter = locked);
            next();
          }));
        } else {
          if (iter.address == -1) {
            designated(null);
          } else {
            stash(iter, iterIndex, validate(callback, function (entry) {
              designated(entry.key)
            }));
          }
        }
      }

      function designated (key) {
        stack.forEach(function (page) { unlock(page) });
        cacheKey(page, page.addresses[index], key);
        callback(null, key);
      }
    } else {
      callback(null, key);
    }
  }

  function unwind (callback) {
    var vargs = __slice.call(arguments, 1);
    if (options.nextTick) process.nextTick(function () { callback.apply(null, vargs) });
    else callback.apply(null, vargs);
  }

  // Binary search implemented, as always, by having a peek at [Algorithms in
  // C](http://www.informit.com/store/product.aspx?isbn=0201314525) by [Robert
  // Sedgewick](http://www.cs.princeton.edu/~rs/).
  //
  // We set `low` to `1` to exclude a deleted ghost first record in a leaf page,
  // or the least child page of a branch page.
  //
  // Index is bitwise compliment of the insert location if not found.
  function find (page, key, low, callback) {
    var mid, high = (page.addresses || page.positions).length - 1, check = validator(callback);

    test();

    function test () {
      if (low <= high) {
        mid = low + ((high - low) >>> 1);
        designate(page, mid, check(compare));
      } else {
        unwind(callback, null, ~low);
      }
    }

    function compare (other) {
      var compare = comparator(key, other);
      if (compare == 0) {
        unwind(callback, null, mid);
      } else {
        if (compare > 0) low = mid + 1;
        else high = mid - 1;
        test();
      }
    }
  }

  // ## Descent
  //
  // We use the term *descent* to describe b&#x2011;tree operations, because all
  // b&#x2011;tree operations require a descent of the b&#x2011;tree, a
  // traversal of the b&#x2011;tree starting from the root. Whenever we are
  // search the tree, insert or delete records, or balance the tree with page
  // splits and merges, we first begin with a descent of the b&#x2011;tree, from
  // the root, to find the page we want to act upon.
  //
  // #### Descent as Unit of Work
  //
  // We use the term descent to describe both the traversal of the b&#x2011;tree
  // and the subsequent actions performed when when the desired page is found.
  //
  // The descent is the unit of work in our concurrency model.  A descent is
  // analogous to a thread, because when a descent waits on I/O, other descents
  // can make progress.
  //
  // Descents can make progress concurrently, even though Node.js only has a
  // single thread of execution. Descents do not actually make progress in
  // parallel, but their progress can be interleaved. When we descend the tree,
  // we may have to wait for evented I/O to read or write a page. While we wait,
  // we can make progress on another descent in the main thread of execution.
  //
  // Because descents can make interleaved progress, we need to synchronize
  // access to b&#x2011;tree pages, just as we would with a multi-threaded
  // b&#x2011;tree implementation.  When we descend the b&#x2011;tree we need to
  // make sure that we do not alter pages that another waiting descent needs to
  // complete its descent when it awakes, nor read pages that a waiting descent
  // had begun to alter before it had to wait.
  //
  // These are race conditions. We use the shared read/exclusive write locks
  // described in the `IO` class above to guard against these race conditions.
  //
  // #### Classes of Descent
  //
  // When we descend to leaf pages of a b&#x2011;tree to obtain records,
  // we *search* the b&#x2011;tree. When we change the size of the b&#x2011;tree
  // by adding or deleting records we *edit* the b&#x2011;tree. When we change
  // the structure of the b&#x2011;tree by splitting or merging pages, we
  // *balance* the b&#x2011;tree.
  //
  // We talk about search descents, edit descents, and balance descents when we
  // describe the interaction of b&#x2011;tree operations.
  //
  // We use these terms in this document to save the chore of writing, and the
  // confusion of reading; insert or delete, or split or merge. We also want to
  // draw a distinction between changing the count of records stored in the
  // b&#x2011;tree, *editing*, and changing the height of the b&#x2011;tree, the
  // count of pages, or the choice of keys, *balancing*.
  //
  // #### Locking on Descent
  //
  // Because a search descent does not alter the structure of the b&#x2011;tree,
  // multiple search descents can be performed concurrently, without interfering
  // with each other.
  //
  // Descents that alter the b&#x2011;tree require exclusive access, but only to
  // the pages they alter. A search descent can still make progress in the
  // presence of an alteration decent, so long as the search does not visit the
  // pages being altered.
  //
  // A search descent obtains shared locks on the pages that it visits.  An
  // alteration descent obtains exclusive locks only on the pages that it needs
  // to alter. The alteration descent will obtain shared locks on the pages that
  // visits in search the pages that it wants to alter.
  //
  // #### Locking Hand Over Hand
  //
  // To allow progress in parallel, we lock only the pages we need to descend
  // the tree, for only as long as we takes to determine which page to visit
  // next. Metaphorically, we descend the tree locking hand-over-hand.
  //
  // We start from the root page. We lock the root page. We perform a binary
  // search that compares our search key against the keys in the root page. We
  // determine the correct child page to visit to continue our search. We lock
  // the child page. We then release the lock on the parent page.
  //
  // We repeat the process of locking a page, searching it, locking a child, and
  // then releasing the lock on the child's parent.
  //
  // We hold the lock on the parent page while we acquire the lock on the child
  // page because we don't want another descent to alter the parent page,
  // invaliding the direction of our descent.
  //
  // #### Lateral Traversal of Leaf Pages
  //
  // Leaf pages are singly linked to their right sibling. If you hold a lock on
  // a leaf page, you are allowed to obtain a lock on its right sibling. This
  // left right ordering allows us to traverse the leaf level of the
  // b&#x2011;tree, which simplifies the implementation of record cursors and
  // page merges.
  //
  // When we move from a leaf page to its right sibling, we hold the lock on the
  // left leaf page until we've obtained the lock on the right sibling. The
  // prevents another descent from relinking our page and invalidating our
  // traversal.
  //
  // #### Deadlock Prevention and Traversal Direction
  //
  // To prevent deadlock between search and mutate descents and balancing
  // descents, when descending the b&#x2011;tree for search or mutation we
  // always traverse a parent branch page to its child. When traversing leaf
  // pages, we always traverse from left to right. By traversing in a consistent
  // order we prevent the deadlock that would occur when another descent was
  // attempting to obtain locks on pages in the opposite order.
  //
  // Because there is only ever one balance descent at a time, and because
  // branch pages are only ever locked exclusively by balance descents, we are
  // allowed to take more liberties when traversing branch pages for the purpose
  // of balancing. We can lock the right sibling of a branch page before locking
  // the branch page because we know that we're the only descent that would move
  // laterally along branch page levels of the b&#x2011;tree.
  //
  // Balance descents can also begin a shared or exclusive descent at any branch
  // page or leaf page in the b&#x2011;tree, so long as they do not already have
  // a branch locked. Because only a balance descents will change the shape of
  // the b&#x2011;tree, it can start anywhere in b&#x2011;tree. A search or
  // mutation descent cannot jump to any page in the b&#x2011;tree because it
  // risks jumping to a page that is the process of being split or merged.
  //
  // As an extra special case, if a balance descent is only performing shared
  // locks in a descent that only includes branch pages, it can lock however
  // many branch pages it likes, in any order. In this case, there are no other
  // descents that would perform an exclusive lock on any branch page, so their
  // is no chance of deadlock. As an added bonus, for this special case, the
  // shared lock implementation is reentrant, so the balance descent can lock a
  // branch page in any order as many times as it likes.

  //
  function Descent (override) {
    // Initialize override.
    override = override || {};

    // The constructor always felt like a dangerous place to be doing anything
    // meaningful in C++ or Java.
    var exclusive = override.exclusive || false,
        depth = override.depth == null ? -1 : override.depth,
        index = override.index == null ? 0 : override.index,
        page = override.page || { addresses: [ 0 ] },
        indexes = override.indexes || {},
        descent = {},
        greater = override.greater, lesser = override.lesser;

    // #### Properties
    //
    // The current branch page or leaf page of the descent.
    function _page () { return page }

    // The index of the child branch page or leaf page in the descent.
    function _index () { return index }

    // The index of the child branch page or leaf page can be assigned to adjust
    // the trajectory of the descent. We use this to descent to the left sibling
    // of a page we want to merge into its left sibling.
    function index_ (i) { indexes[page.address] = index = i }

    // A map of the address of each page so far visited in the path to the index
    // of the child determined by the navigation function.
    function _indexes () { return indexes }

    // The current depth of the descent into the b&#x2011;tree where `-1`
    // indicates no descent and `0` indicates the root.
    function _depth () { return depth }

    // An instance of `Descent` that if followed to the right to the `depth` of
    // this `Descent` will arrive at the left sibling of the current page of
    // this `Descent`. If the current `page` has no left sibling, then the
    // `lesser` property is undefined.
    function _lesser () { return lesser }

    // An instance of `Descent` that if followed to the left to the `depth` of
    // this `Descent` will arrive at the right sibling of the current page of
    // this `Descent`. If the current `page` has no right sibling, then the
    // `greater` property is undefined.
    function _greater () { return greater }

    // #### Forking
    //
    // We use `fork` to create a new descent using the position of the current
    // descent. The new descent will continue to descend the tree, but without
    // releasing the lock on the page held by the descent from which we forked.
    //
    // When we split and merge pages other than the root page, we need to hold
    // locks on pages at more than one level of the tree. Pages at the parent
    // level will have nodes inserted or deleted. Pages at the child level will
    // be split or merged. When we reach a parent, we use `fork` to create a new
    // descent, so we don't release our lock on the parent when we visit the
    // child.
    //
    // When we merge and delete leaf pages, we also need to update the key,
    // which may be at any level of the tree. We need to hold our lock on the
    // branch page that contains the key, while still descending the tree to
    // find the two pages that need to be merged. The two pages to merge may not
    // be immediate children of the same penultimate branch page.
    //
    // **TK** Glurgh: We need to leave the key page locked, then go left and
    // right to find two separate pages. We do not need to hold locks on all the
    // pages down to the targets, just the pivot point and the targets  The
    // hand-over-hand logic works fine. Descending hand-over-hand exclusive will
    // cause us to wait for other descents to finish, squeezing the other
    // descents out.

    // We create a new `Descent` which creates a dummy first page. We then
    // assign the addresses current descent to the dummy page, and copy the
    // current index.

    //
    function fork () {
      return new Descent({
        page: page,
        exclusive: exclusive,
        depth: depth,
        greater: greater,
        lesser: lesser,
        index: index,
        indexes: extend({}, indexes)
      });
    }

    // #### Excluding

    // All subsequent locks acquired by the descent are exclusive.
    function exclude () { exclusive = true }

    // Upgrade a lock from shared to exclusive. This releases the lock on the
    // current branch page and reacquires the lock as an exclusive. It only
    // works only with branch pages, and it only works because the only descent
    // that acquires an exclusive lock on a branch page is a balancing descent.
    //
    // All subsequent locks acquired by the descent are exclusive.
    function upgrade (callback) {
      unlock(page);

      lock(page.address, exclusive = true, validate(callback, locked));

      function locked (locked) {
        page = locked;
        callback(null);
      }
    }

    // #### Navigating
    //
    // The `descend` function accepts a navigation function that determines the
    // index of the child to follow down the tree.

    // Follow the path for the given key.
    function key (key) {
      return function (callback) {
        var found = find(page, key, page.address % 2 ? page.ghosts : 1, callback);
        return found;
      }
    }

    // Always goes down the left most path.
    function left (callback) { callback(null, page.ghosts || 0) }

    // In the method you're about read, please don't let the `or` operator
    // bother you, although it has in the past. When I set out to rename the
    // addresses array of the branch page and the positions array of the leaf
    // page so that they both the had a references array, the documentation
    // became far more verbose as it set out to describe the meaning of the
    // hopelessly generic term reference.
    //
    // It is bound to cause confusion to readers who see that both branch pages
    // and leaf pages have a references array, but the contents of the array
    // mean something different, yet some of the algorithms treat them as the
    // same. Where this occurs, where you're able to use the same algorithm for
    // both page addresses and record positions, it is actually easier to
    // understand when you see the `or` operator.
    //
    // You'll see this use of the `or` operator to choose between addresses and
    // positions in three places in the source.

    // Follow the right most path.
    function right (callback) { callback(null, (page.addresses || page.positions).length - 1) };

    // #### Stopping
    //
    // The `descend` function accepts a stop function that determines when to
    // terminate the descent.

    // Stop when we've reached the branch page that contains a child whose key
    // matches the any of the given keys.
    function found (keys) {
      return function () {
        return page.addresses[0] != 0 && index != 0 && keys.some(function (key) {
          return comparator(page.cache[page.addresses[index]],  key) == 0;
        });
      }
    }

    // Stop before a we descend to a child with a certain address.
    function child (address) { return function () { return page.addresses[index] == address } };

    // Stop when we reach a specific page identified by its address.
    function address (address) { return function () { return page.address == address } };

    // Stop when we reach a penultimate branch page.
    function penultimate () { return page.addresses[0] % 2 }

    // Stop when we reach a leaf page.
    function leaf () { return page.address % 2 }

    // Stop when we reach a certain depth in the tree relative to the current
    // depth.
    function level (level) {
      return function () { return level == depth }
    }

    // #### Unlocking
    //
    // Ordinarily, we unlock as we descent skipping the initial branch page. In
    // the case of a descent from the root, the initial branch page is a dummy
    // branch page containing the root. In the case of a forked descent, the
    // forked descent does not own the branch page from which the descent
    // forked.
    var unlocking = false;

    function unlocker (parent) {
      if (unlocking) unlock(parent);
      unlocking = true;
    }

    // #### Uncaching
    //
    // When we descend to remove a leaf page for a merge, to change the key
    // value of a leaf page, we want to discard any cached references to the key
    // value in the branch pages on the path to the leaf page. This property is
    // turned off by default and it is not inherited by a fork.
    var uncaching = false;
    function uncaching_ ($uncaching) { uncaching = $uncaching }

    // However, there is the special case of a descent to delete a page as a
    // result of a  merge where we're going to want to hold onto the locks to
    // multiple pages on the path to the page we want to delete. The merge
    // function provides it's down unlocker function.
    function unlocker_ ($unlocker) { unlocker = $unlocker }

    // #### Descending
    //
    // Descent the b&#x2011;tree from the current branch page using the given
    // navigation function `next` to determine the page to take and the given
    // stop function `stop` to determine when to stop the descent.
    function descend (next, stop, callback) {
      var check = validator(callback), above = page;

      downward();

      function downward () {
        if (stop()) {
          unwind(callback, null, page, index);
        } else {
          // We'll only ever go here if we're at a branch page.
          if (index + 1 < page.addresses.length) {
            greater = fork();
            greater.index++;
          }
          if (index > 0) {
            lesser = fork();
            lesser.index--;
          }
          lock(page.addresses[index], exclusive, check(locked));
        }
      }

      function locked (locked) {
        depth++;
        unlocker(page, locked);
        page = locked;
        next(check(directed));
      }

      function directed ($index) {
        if (!(page.address % 2) && $index < 0) {
          index = (~$index) - 1;
        } else {
          index = $index;
        }
        indexes[page.address] = index;
        if (uncaching && !(page.address % 2)) {
          uncacheKey(page, page.addresses[index]);
        }
        downward();
      }
    }

    // Construct the `Descent` object and return it.
    return classify.call(this, descend, fork, exclude, upgrade,
                               key, left, right,
                               found, address, child, penultimate, leaf, level,
                               _page, _depth, _index, index_, _indexes, _lesser, _greater,
                               uncaching_, unlocker_);
  }


  // ## Cursors
  //
  // Application developers navigate the b&#x2011;tree using one of two types of
  // ***cursor***. To read records in the b&x2011;tree they use an
  // ***iterator***. To read records, as well as insert and delete records, they
  // use ***mutator***.
  //
  // An iterator provides random access to the records in a page. It can move
  // from a page to the right sibling of the page. A mutator does the same, but
  // it is also able to insert or delete records into the current page.
  //
  // ### Iterator
  //
  // The application developer uses an iterator to move across the leaf pages of
  // the b&#x2011;tree in ascending collation order, one leaf page a time.
  //
  // #### Search Keys
  //
  // The application developer obtains an iterator by calling `Strata.iterator`
  // with a ***search key***.  The search key is used to find the leaf page
  // where the record from which the key is derived belongs in the
  // b&#x2011;tree. The record may not actually exist in the b&#x2011;tree, in
  // which case the iterator begins with the leaf page where record *would* be.
  //
  // #### Page by Page
  //
  // The leaf pages themselves are visited one at a time, not the records. The
  // iterator can randomly access any record in the currently visited leaf page.
  //
  // When a page is visited it is read locked, so that other descents can visit
  // the page, but they cannot insert or delete records. By locking the pages
  // left to right hand over hand, then there is no way for the tree to mutate
  // such that would defeat our iteration. Leaf pages that we've visited may by
  // edited by another descent after we've visited them, however.
  //
  // **TK**: Definition of keys and records. Go back up and make sure one is
  // there.
  //
  // #### Record Ranges
  //
  // The cursor will define an `offset` property and a `length` property. The
  // `offset` is positioned at the first record in the page whose key is equal
  // to or greater than the search key. The `length` is the count of records in
  // the page. This defines the range of records whose key is greater than or
  // equal to the search key.
  //
  // On the first page visited, the key of the record at the `index` is greater
  // than or equal to the search key. Every key of every record that follows the
  // record at the index is greater than the search key.
  //
  // #### Full Tree Iteration
  //
  // The application developer can also obtain an iterator that begins at the
  // left most leaf page by calling `Strata.iterator` without a search key. This
  // positions the iterator at the first leaf page in the b&#x2011;tree and the
  // index at the first record in b&#x2011;tree.
  //
  // #### Double Loop
  //
  // The interface to iteration requires the application developer to implement
  // double loop to traverse the leaf pages. The outer loop moves from page to
  // page. The inner loop moves from record to record. The iterator interface
  // does not hide the underlying structure of the leaf pages.
  //
  // It is not intended to be an abstraction. It is intended to expose the
  // structure. Do not confuse the iterator with an iterator from other APIs
  // that exposes one item at a time. Our iterator exposes a range of records.
  //
  // #### Ranged Searches
  //
  // Ranged searches are performed by searching for the start of the range and
  // iterating to the end of the range. There is nothing to this. It is how
  // iterator is implemented.
  //
  // We may be interested in searching for time series data that occurred
  // between noon and midnight, where are time stamp is POSIX time, milliseconds
  // since the epoch.  We create an iterator with a search key that is noon
  // according to POSIX time. It doesn't matter to us if there were no events
  // that occurred exactly at the millisecond that defines noon. Our iterator
  // begins at the point that is either an event that occurred millisecond that
  // defines noon, or else the first event that occurred after the noon
  // millisecond.
  //
  // When we encounter to the first event that occurs after midnight, we ignore
  // that event and terminate traversal. We've successfully found all the events
  // in our range.

  //
  function Cursor (exclusive, searchKey, page, index) {
    // Iterators are initialized with the results of a descent.
    var rightLeafKey = null
      , length = page.positions.length
      , offset = index < 0 ? ~ index : index
      ;

    // Get a record at a given index from the current leaf page.
    function get (index, callback) {
      stash(page, index, validator(callback)(unstashed));
      function unstashed (entry) { toUserLand(callback, null, entry.record) }
    };

    // Go to the next leaf page, the right sibling leaf page. Returns true if
    // there is a right sibling leaf page of the current page, false if there
    // the current leaf page is the last leaf page in the b&#x2011;tree.
    function next (callback) {
      var next;

      // Clear out peek at the next page.
      rightLeafKey = null;

      // If we are not the last leaf page, advance and return true.
      if (page.right) {
        lock(page.right, exclusive, validate(callback, locked));
      } else {
        toUserLand(callback, null, false);
      }

      function locked (next) {
        //  Unlock the current page.
        unlock(page);

        // Advance to the next page.
        page = next

        // Adjust the range.
        offset = page.ghosts;
        length = page.positions.length;

        // We have advanced.
        toUserLand(callback, null, true);
      }
    }

    // Get the index of the record from which the given key is derived, or else
    // the bitwise compliment of index where record would be inserted if no such
    // record exists in the leaf page.
    function indexOf (key, callback) {
      find(page, key, page.ghosts, callback);
    }

    // Unlock all leaf pages held by the iterator.
    function unlock () {
      _unlock(page);
    }

    function _index () { return index }

    function _offset () { return offset }

    function _length () { return length }

    classify.call(this, unlock, indexOf, get, next, _index, _offset, _length);

    if (!exclusive) return this;

    // ### Mutator

    // A mutator is an iterator that can also edit leaf pages. It can delete
    // records from the currently visit leaf page. It can insert records into
    // the current leaf page, if the record belongs in the current leaf page.
    //
    // As with `Iterator`, it moves across the leaf pages of the b&#x2011;tree
    // in ascending collation order. It has random access to the records in the
    // page using an index into the array of records.
    //
    // As with `Iterator`, the application developer obtains an iterator by
    // calling `Strata.mutator` with a search key.
    //
    // #### Ranged Inserts
    //
    // You can insert a range of records using a single mutator. This is
    // efficient if you have a range of records whose keys are close together,
    // maybe so close that they are all on the same page, so you can descend the
    // tree to the correct page and insert them in one fell swoop.
    //
    // #### Ambiguous Insert Locations
    //
    // Insert locations for ranged inserts can be ambiguous if the binary search
    // indicates that a record should be inserted at the end of the leaf page,
    // after the current last record in the leaf page. If the insert location is
    // after the last record, it could be the case that the record really
    // belongs to a right sibling leaf page.
    //
    // This is only a problem when we insert a record whose key is not the key
    // used to create the mutator. An insert location is always unambiguous if
    // the key is the search key used to locate the first page. The key is
    // determined to belong inside the leaf page by virtue of a descent of the
    // b&#x2011;tree. That is unambiguous.
    //
    // To know if a subsequent record insert really does belong after the last
    // record but before the first record of the right sibling leaf page, we
    // have to load the right sibling leaf page and peek at the record. When we
    // do this, we need to keep the right sibling leaf page locked, so that the
    // key of the right sibling page cannot change.
    //
    // This peek has a cost. If you are inserting a range, and the records are
    // more often pages apart from each other than they are on the same page, it
    // might not be worth it to peek. It might be more efficient to assume that
    // the next record is much further along and create a new mutator for the
    // remainder of the insert range. In this case we're saying, see if you can
    // insert the next record on this page as long as we're here, but we leaf
    // traversal is inefficient for our range,  so don't try too hard.
    //
    // If we are only inserting a single record, there's no ambiguity, because
    // we'll use the key of the record to create the mutator. There is no need
    // to enable peek for a single insert, but there is no real cost either.
    //
    // #### Duplicate Keys
    //
    // Although duplicate keys are not allowed, abstracted duplicate keys are
    // not difficult for the application developer to implement. One simply adds
    // a hidden series value to the key. This creates a series of records in the
    // leaf pages that have an identical user key disambiguated by the series
    // value.
    //
    // Given a key plus a maximum series value, you will always land after the
    // last one the series, or else a record that is less than the key, which
    // means that the series is zero.
    //
    // The maximum series value is a module developer choice, so chose a
    // `Number`. That way you can search using `Number.MAX_VALUE` as the series
    // value and you will always land at the spot where a new record should be
    // appended. The correct series value to store is the series value of the
    // record at the given insert index plus one if the key is identical, zero
    // if it is not.
    //
    // **TODO**: Zero is a valid index for the left most leaf page.
    //
    // #### Insert

    // As noted elsewhere, you will provide an insert index, but you will
    // provide one that you've obtained from either the `Cursor` or through a
    // call to `indexOf`. Get the correct index, then toddle over to `insert`
    // and give `insert` the correct index.
    //
    // Why is this index not encapsulated? Because the descent will have
    // performed a binary search on the leaf, stored the result in the `index`
    // property of the cursor, and we don't want to repeat that binary search
    // because we're stingy.
    //
    // At some point, you're going to want come back and reconsider
    // encapsulation. Please, don't. Before long, we're going to want to expose
    // more the properties of the b&#x2011;tree, not hide them. Encapsulation is
    // not your friend. It makes you look at this data structure as a black box,
    // and it is not a black box.

    //  &mdash;

    // Insert the record with the given key at the given index. Obtain the
    // correct index to preserve the sort order by calling `indexOf`.
    //
    // If the index is after the last record in the leaf page, this method will
    // check that the record does not actually belong in a subsequent sibling
    // leaf page.
    //
    // If the insert index of the record is `0` and this is not the first leaf
    // page of the tree, `insert` will return `-1` indicating that the record
    // belongs in a leaf page to the left of the current leaf page. This is
    // unambiguous, but it will always require that you descend the tree to find
    // the correct leaf leaf page for insert.
    //
    // If there is a right sibling leaf page, it will load the right sibling
    // leaf page and check that the leaf is less than the key of the right
    // sibling leaf page. If the key of the insert record is greater than the
    // key of the right sibling leaf page, then the record does not belong in
    // this leaf page. The record will not be inserted. The method returns `1`
    // indicating that the record belongs in a leaf page to the right of the
    // current leaf page. Because the right leaf page key is cached, the right
    // leaf page key may change, so the `+1` result is probably correct,
    // possibly incorrect, but the correct course of action it to descend the
    // tree again which is always correct and resolves the ambiguity.
    //
    // If the insert index of the record is after the last record, and upon
    // peeking at the first record of the right sibling leaf page we determine
    // that the record belongs on a subsequent page, `insert` will return
    // `false` indicating that the record does not belong in the current leaf
    // page.
    //
    // This method will happily accept all other forms of invalid data. The
    // application developer is responsible for maintaining the collation order
    // of the leaf page. The application developer must not insert duplicates.
    // The application developer must make sure to provide a `record`, `key` and
    // `index` that correspond to each other. No assertions are performed on the
    // validity of the insert.
    //
    // #### Avoiding the Peek
    //
    // There is a cost involved with peeking at the right sibling leaf page to
    // determine if a record greater than the greatest record in the current
    // leaf page belongs in the current page, or in a subsequent leaf page. If
    // the application developer doesn't want to peek, they can take matters
    // into their own hands. They can determine the insert location using
    // `indexOf`, and if it is after the last record, they can use a new mutator
    // to find the insert location of the next page.
    //
    // There is no cost involved when inserting a range into the last leaf page,
    // a common operation, because the right sibling leaf page does not exist,
    // so there is no doubt that the records belong on the last page.

    //
    function insert (record, key, index, callback) {
      var check = validator(callback), unambiguous;

      // On every leaf page except the first leaf page of the descent (not of
      // the entire tree), the least record is the key, and inserted records are
      // always greater than the key. Thus, if we have an index of zero and we
      // are not the first leaf page, the record does not belong in this leaf
      // page.
      if (index == 0 && page.address != 1) {
        toUserLand(callback, null, -1);
        return;
      }

      // An insert location is ambiguous if it would append the record to the
      // current leaf page.
      unambiguous = index < page.positions.length;

      // If we are at the first leaf page and the key is the search key that got
      // us here, then this is, without a doubt, the correct leaf page for the
      // record.
      unambiguous = unambiguous || searchKey.length && comparator(searchKey[0], key) == 0;

      // An insert location is unambiguous if  we are the last page. There is no
      // subsequent page to which the record could belong.
      unambiguous = unambiguous || ! page.right;

      // If insert location is unambiguous, insert the record and return the
      // insert index, otherwise return `undefined`.

      //
      if (unambiguous) insert ();
      else ambiguity();

      // An insert location is ambiguous if we have an ambiguous insert
      // location, peek at the next leaf page to see if the record doesn't
      // really belong to a subsequent leaf page.
      function ambiguity () {
        // We used to believe that the lock must held because the balancer can
        // swoop in and prune the ghost first records and thereby change the
        // key. It could not delete the page nor merge the page, but it can
        // prune dead first records.
        //
        // Now we realize that this doesn't mean that the lock must be held. The
        // only thing that a prune of the ghost first record will do is cause
        // the key to *increase* in value. Thus, even if it changes, our test to
        // determine if a record belongs in this leaf page will only ever err on
        // the side of caution. It will not falsely say that the record belongs
        // on the current page, but it may falsely say that the record does not
        // belong on the current page. Definately yes, probably no.
        //
        // In the case of a no, it might really be a yes, but it doesn't mater.
        // Just descend the tree again with the maybe no key. Descending the
        // tree with a key is always the safe bet.

        //
        if (rightLeafKey) {
          compare();
        } else {
          lock(page.right, false, check(load));
        }

        function load (rightLeafPage) {
          stash(rightLeafPage, 0, check(designated));

          function designated (entry) {
            rightLeafKey = entry.key;
            _unlock(rightLeafPage);
            compare();
          }
        }

        function compare () {
          if (comparator(key, rightLeafKey) < 0) insert();
          else toUserLand(callback, null, +1);
        }
      }

      function insert () {
        var fd;

        // Cache the current page length.
        balancer.unbalanced(page);

        // Since we need to fsync anyway, we open the file and close the file
        // when we append a JSON object to it. Because no file handles are kept
        // open, the b&#x2011;tree object can left to garbage collection.
        fs.open(filename(page.address), "a", 0644, check(write));

        function write ($) {
          writeInsert(fd = $, page, index, record, check(written));
        }

        function written (position, lengths) {
          // Insert the position into the page a cache the record.
          splice('positions', page, index, 0, position);
          splice('lengths', page, index, 0, lengths);
          cacheRecord(page, position, record, key);

          // Update the length of the current page.
          length = page.positions.length;
          fs.close(fd, check(close));
        }

        function close () {
          toUserLand(callback, null, 0);
        }
      }
    }

    // Delete the record at the given index. The application developer is
    // responsible for providing a valid index, in the range defined by the
    // `offset` and `length` of the cursor, or else the `ghosts` and `length` of
    // the `page`.
    function remove (index, callback) {
      // If we're deleting the leaf page key, we ghost the key.
      var ghost = page.address != 1 && index == 0
        , check = validator(callback)
        , fd
        ;

      // Record the page as unbalanced.
      balancer.unbalanced(page)

      // Append a delete object to the leaf page file.
      fs.open(filename(page.address), "a", 0644, check(opened));

      function opened ($) {
        writeDelete(fd = $, page, index, check(written));
      }

      function written () {
        // If we've created a ghost record, we don't delete the record, we
        // simply move the `ghosts` for the page forward to `1`. If the current
        // offset of the cursor is `0`, we move that forward to `1`. Otherwise,
        // we uncache and splice the record.
        if (ghost) {
          page.ghosts++;
          offset || offset++;
        } else {
          uncacheRecord(page, page.positions[index]);
          splice('positions', page, index, 1);
          splice('lengths', page, index, 1);
// **FIXME**:          length = page.length
        }
        fs.close(fd, check(closed));
      }

      function closed () {
        toUserLand(callback, null);
      }
    }

    return classify.call(this, insert, remove);
  }

  // #### Insertion and Deletion Versus Balance
  //
  // We do not attempt to balance the tree with every insertion or deletion. The
  // client may obtain a cursor to the leaf pages, iterate through them deleting
  // records along the way. As the client alters leaf records, they are marked
  // as candidates for balance. Balancing will take place periodically, where a
  // single thread of control **TK** will
  //
  // #### Staccato Balance Operations
  //
  // The b&#x2011;tree balance operations cascade by nature. If you insert a
  // value into a leaf node, such that the leaf node is beyond capacity, you
  // split the leaf node, adding a new child to the parent node. If the parent
  // node is now beyond capacity, you split the parent node, adding a new child
  // to its parent node. When every node on the path to the leaf node is at
  // capacity, a split of the leaf node will split every node all they way up to
  // the root.
  //
  // Merges too move from leaves to root, so that a merge at one level of the
  // b&#x2011;tree potentially triggers a merge of the parent with one of its
  // siblings.
  //
  // However, we've established rules for lock acquisition that require that
  // locks are obtained from the top down, and never from the bottom up. This is
  // why we do not perform balance operations as a part of a single pass. We
  // instead descend the tree once to insert or delete records form the leaf
  // pages. We then descend the tree once for each split or merge of a page.
  //
  // Much b&#x2011;tree literature makes mention of a potential efficiency where
  // you split full pages on the way back up from an insert. You can determine
  // which pages would split if the leaf split as you descend the b&#x2011;tree,
  // since you'll visit every page that would participate in a split.
  //
  // That efficiency applies only for split, and not for merge, because you have
  // to inspect the left and right siblings of a page to determine if it is time
  // to merge. If the left sibling page of a page, is not also child of that
  // page's parent page, then the left sibling page is in a different
  // sub&#x2011;tree. It can not be reached by the path that was used to find
  // the leaf page where the delete occurred.
  //
  // The single pass insert on the way down and split on the way up violates the
  // rules we laid out to prevent deadlock. To abide by our rules, we'd have to
  // lock exclusively on the way down, then hold the locks on the pages above
  // the leaf page that were full and could possibly split. This would reduce
  // the liveliness of our implementation.
  //
  // There are compromises, but rather than create a complicated locking
  // apparatus, with upgrades, we're going to simplify our algorithm greatly, by
  // descending the tree once for each split or merge.
  //
  // When we travel to the unbalanced page, we acquire shared locks in the hand
  // over hand fashion used for search. We acquire exclusive locks only on those
  // pages that participate in the balance operation. That is two pages in the
  // case of the split. In the case of a merge that is three pages. During a
  // balance operation are locking exclusively, at most, three pages at a time.
  //
  // If out balance operation cascades so that it requires a balance at every
  // level, we'll descend the tree once for every level. However, the path we
  // follow is almost certain to be in memory, since we're revisiting the same
  // path.
  //
  // Also, a balance operation will involve an increasing number of levels with
  // decreasing frequency. A split will most often require that only the leaf
  // page is split. The penultimate pages will be involved in a balance
  // operation at least an order of magnitude less frequently. The pages above
  // the penultimate branch pages will be involved in a balance operation yet
  // another order of magnitude less frequently.
  //
  // Conserving descents during balance operations is a false economy. It
  // complicates lock acquisition. It reduces the liveliness of the
  // b&#x2011;tree.
  //
  // The multiple descents will allow searches of the b&#x2011;tree to make
  // progress between balance operations.
  //
  // ##### Descent as Unit of Work
  //
  // We can see that a descent of the tree is analogous to a thread in
  // multi-threaded operation. A decent is an actor on the tree, performing a
  // single balance operation, searching
  //
  // ##### Delayed Balance
  //
  // We've determined that we do not want to perform a split or merge of the
  // leaf level the moment we've detected the need for one. If we fill a leaf
  // page, we descend the tree again to split the leaf page.
  //
  // Because the b&#x2011;tree is a concurrent structure, the leaf split descent
  // may discover that another descent has removed a record, and a leaf split is
  // no longer necessary. There may be, in fact, a descent on the way to the
  // left sibling of the page, to check for the potential for a merge.
  //
  // The concurrent operation means that we have to deal with situation where
  // we've undertaken a descent to balance the b&#x2011;tree, but another series
  // of descents has rendered that plan invalid.
  //
  // As long as we're dealing with that problem, we may as well decouple
  // insertion and deletion form split and merge entirely, and see if we can't
  // gain more liveliness, and a simpler implementation, by separating these
  // concerns.
  //
  // We can provide an interface where the application developer can insert or
  // delete any number of records, then order a balance of the tree that takes
  // all the changes into account. This can avoid degenerate cases of sort where
  // a leaf page at the split threshold and the application in turn inserts and
  // deletes a single record from it.
  //
  // We can provide the application developer with a cursor. The cursor can
  // delete a range of values, or insert a range of values, without having to
  // descend the tree for each inserted value. The developer can insert or
  // delete records as fast as the operating system can append a string to a
  // file. We'll balance this mess for her later.
  //
  // It is still cheap to check for balance for single inserts or deletes, as if
  // we were checking as part of a single pass.
  //
  // ##### Balance Thread
  //
  // We perform balance operations one a time. We do not begin a new balance
  // operation until the previous one completes. In essence, we perform
  // balancing in a separate thread of execution.
  //
  // Of course, a Node.js application really only has one thread of execution.
  // In our b&#x2011;tree, however, multiple descents can make progress at the
  // time, or rather, the progress made by one decent, while another descent
  // waits on I/O.
  //
  // We ensure that only one descent at a time is making progress toward the
  // balance of the tree. This simplifies or balance implementation, because the
  // structure of the tree, its depth and number of pages, will only be changed
  // one one series of descents. A balance descent can assume that the tree
  // retain its structure while the balance descent waits on I/O.
  //
  // ##### Balancer
  //
  // We will call the code that co-ordinates the series of splits and merges to
  // balance the tree, the *balancer*.
  //
  // The balancer maintains an offset count for each modified leaf page in the
  // b&#x2011;tree. When an insert is performed, the offset count for the leaf
  // page is incremented.  When a delete is performed, the offset count for the
  // leaf page is decremented. This keeps track of the total change in size.
  //
  // We use the offset counts to determine which pages changed.
  //
  // ##### Balance Cutoff
  //
  // When it comes time to balance, we create a new balancer and put it in place
  // to collect a new round of offset counts, while we are off balancing the
  // pages gathered in the last round balance counts.
  //
  // We create a balance plan for the current set of pages. We balance the tree,
  // splitting and merging pages according to our balance plan. Only one
  // balancer balances the tree at a time. The balancer perform one split or
  // merge descent at a time. Balance descents are never concurrent with other
  // balance descents.
  //
  // While balancing takes place, records can be inserted and deleted
  // concurrently. Those changes will be reflected when the next balancer
  // balances the tree.
  //
  // ##### Creating a Balance Plan
  //
  // When it comes time to balance, we consult the pages for which we've
  // maintained the offset counts. If the page is greater than the maximum page
  // size, we split the page. That much is obvious. Otherwise, if the offset
  // count is negative, we've deleted records. There may be an opportunity to
  // merge, so we check the left and right siblings of the page to determine if
  // a merge is possible.
  //
  // Determining a plan for a merge requires inspecting three pages, the page
  // that decreased in size, plus its left and right sibling pages. We merge a
  // page with the sibling that will create the smallest page that is less than
  // or equal to the maximum page size.
  //
  // ##### Purging the Cache
  //
  // When inspecting page for merge, we are only interested in the count of
  // records in the page. It may have been a long time since the last merge, so
  // we might have accumulated a lot of pages that need to be consulted. To
  // create a complete plan, we'll need to gather up the sizes of all the leaf
  // pages, and the only way to get the size of a page is to load it into
  // memory. But, we won't need to keep the page in memory, because we only need
  // the size.
  //
  // When we calculate a merge, we load the left and right sibling. We're
  // touching a lot of pages that we don't really know that we need.
  //
  // When we lock a page, we indicate that if the page is loaded, it ought to be
  // loaded into balancer most-recently used list. This indicates that the page
  // was loaded by the balancer. We also set the balancer flag, indicating that
  // we need to preserve the page for record count, even if the actual page data
  // is discarded by a cache purge.
  //
  // We check the cache size frequently. If we're going over, we offer up the
  // balancer pages to the cache purge first. If we determine that a page will
  // be used in a balance operation we add it to the core most-recently used
  // list, where it is subject to the same priority as any other page loaded by
  // the cache.
  //
  // ### Splitting
  //
  // Splitting is the simpler of the two balancing operations.
  //
  // To split a leaf page, we start by obtaining the key value of the first
  // record. We can do this by acquiring a read lock on the page, without
  // performing a descent. The balancer gets to break some rules since it knows
  // that we know that the b&#x2011;tree is not being otherwise balanced.
  //
  // We descend the tree until we encounter the penultimate branch page that is
  // the parent of the branch page.  We acquire an exclusive lock the branch
  // page. We can release our shared lock and acquire an exclusive lock. We do
  // not have retest conditions after the upgrade, because only the balancer
  // would change the keys in a branch page, and we're the balancer.
  //
  // We allocate a new leaf page. We append the greater half of the record to
  // the page. We add the page as a child to the penultimate branch page to the
  // right of the page we've split. We unlock the pages.
  //
  // We can see immediately if the penultimate page needs to be split. If it
  // does, we descend the tree with the same key, stopping at the parent of the
  // penultimate page. We will have kept the address of the parent of the
  // penultimate page for this reason. We split the penultimate page, copying
  // the addresses to a new right sibling. Adding the right sibling to the
  // parent. We make sure to clear the cache of keys for the addresses we are
  // removing. (Oh, and clear the cache for the records when you leaf split. Oh,
  // hey, copy the records as well, duh.)
  //
  // Note that a page split means a change in size downward. It means that one
  // or both of our two halves may be a candidate to merge with what were the
  // left and right siblings of the page before it split. There may have been
  // less than half a page of records one or both both of the sides of the tree.
  // After we split, we check for a chance to merge. More live lock fears, but
  // one heck of a balanced tree.
  //
  // ### Need to Add
  //
  // We always balance cascading up. Unlike leaf pages, which we can allow to
  // deplete, the branch pages cannot be allowed to become empty as we merge
  // leaf pages. As we delete records, we still keep a ghost of a key. As we
  // delete leaf pages, we delete the ghost keys. Branch pages become paths to
  // nowhere. They don't hold their own keys, so they can't find them.  We'd
  // have to have null keys in our tree. Even if we kept keys around, we're
  // sending searches down a path to nowhere. There is no leaf page to visit. We
  // get rid of these paths. We always balance the upper levels immediately, we
  // perform the cascade. Our tree descent logic would have to account for these
  // empty sub&#x2011;trees. Much better to balance and keep things orderly.
  //
  // This raises a concerns about live lock, that we might be balancing
  //
  // **TK**: Yes file times are enough. Even if the system clock changes
  // drastically, the file times are all relative to one another. It it changes
  // during operation, that is a problem, but we're not going to endeavor to
  // provide a solution that deals with erratic clock times. Worst case, how do
  // we not detect a file in need of recovery? We ignore files older than the
  // time stamp file. So, we might have the system clock move far backward, so
  // that the time stamp file is much newer than all the files that are being
  // updated. Oh, well. What's the big deal then? How do we fix that? If it is a
  // server install, we demand that you maintain your clock. If it is a desktop
  // install, we can comb the entire database, because how big is it going to
  // be?
  //
  // Hmm... What are you going to do? This is why people like servers.
  //
  // ### Merging
  //
  // **TODO**: Great example floating around. Imagine that you've implemented
  // MVCC. You're always appending, until it is time to vacuum. When you vacuum,
  // you're deleting all over the place. You may as well do a table scan. You
  // might choose to iterate through the leaf pages.  You may have kept track of
  // where records have been stored since the last vacuum, so if you have a
  // terabyte large table, you're only vacuum the pages that need it.
  //
  // Merge is from right to left. When we merge we always merge a page into its
  // left sibling. If we've determined that a page from which records have been
  // deleted is supposed to merge with its right sibling, we apply our merge
  // algorithm to the right sibling, so that is is merged with its left sibling.
  //
  // When we compare a page from which records have been deleted against its
  // siblings, if the left sibling is to be merged, we use the page itself, the
  // middle page in our comparison. If the middle page is to merge with the
  // right page, we use the right page.
  //
  // To merge a leaf page, we start by obtaining the key value of the first
  // record. We can do this by acquiring a read lock on the page, without
  // performing a descent. The balancer gets to break some rules since it knows
  // that we know that the b&#x2011;tree is not being otherwise balanced.
  //
  // With that key, we descend the b&#x2011;tree. We know that the key value
  // cannot change, because it is the balancer that alters keys. The first
  // record may be deleted by editing, but a ghost of the first record is
  // preserved for the key value, which is the key value of the page.
  //
  // When we encounter the key value in a branch page, we acquire an exclusive
  // lock the branch page. We can release our shared lock and acquire an
  // exclusive lock. We do not have retest conditions after the upgrade, because
  // only the balancer would change the keys in a branch page, and we're the
  // balancer.
  //
  // We then descend the child to the left of the key, instead of to the right
  // as we would ordinary. We descend to the left child acquiring, an exclusive
  // lock, but retaining our exclusive lock on branch page where we found our
  // key. We then descend to the right most child of every child page, acquiring
  // exclusive locks in the hand-over-hand fashion, until we reach a leaf page.
  // We are now at the left sibling of the page we want to merge.
  //
  // We've locked the branch page that contains the key exclusively so that we
  // can reassign the key. It will no longer be valid when the page is merged
  // into its left sibling because the first record is now somewhere in the
  // midst of the left sibling. We lock exclusively hand-over-hand thereafter to
  // squeeze out any shared locks. Our exclusive lock on the parent containing
  // the key prevents another descent from entering the sub&#x2011;tree where we
  // are performing the merge.
  //
  // We now proceed down the path to the merge page as we would ordinarily,
  // except that we acquire exclusive locks hand-over-hand instead of shared
  // locks. This will squeeze out any other descents.
  //
  // We retain the exclusive lock on the penultimate branch page. No other
  // descent will be able to visit this penultimate branch, because we've
  // blocked entry into the sub&#x2011;tree and squeeze out the other descents.
  // We still need to hold onto the exclusive lock, however, otherwise the page
  // might be discarded during a cache purge, which can happen concurrently.
  //
  // We append the records in the merge page to its left sibling. We remove the
  // address of the merged page from the penultimate page.
  //
  // If we've deleted the first child of the penultimate branch page, then we
  // delete the cached key of the new first child. The new first child is the
  // left-most child of the penultimate page. Its key, if it not the left-most
  // page of the entire tree, has been elevated to the exclusively locked branch
  // page where we encountered the merge page key. We don't want keys to gather
  // where they are not used. That is a memory leak.
  //
  // We clear the merge key from the branch page where we found it. The next
  // descent that needs it will look up the new merge key. If we found the merge
  // key in a penultimate page, we need to make sure to clear the key using the
  // page address we stashed, because the page is now deleted.
  //
  // #### Merging Parent Branches
  //
  // Once we've merged a leaf page, we check to see if the penultimate branch
  // page that lost a child can be merged with one of its siblings. The
  // procedure for merging branch pages other than the root branch page is the
  // same regardless of the depth of the branch page.
  //
  // We acquire the key for the left most page in the sub&#x2011;tree underneath
  // the branch page. We do this by following the left most children until we
  // reach a leaf page. We use that key to descend the tree.
  //
  // we lock the page exclusively. We retain that lock for the duration of the
  // merge.
  //
  // When we encounter the key, we descent the child to the left of the key,
  // then we decent the right most child of every page until we reach the page
  // at the same depth as the merge page. That is the left sibling of the merge
  // page.
  //
  // We can then obtain a size for the left sibling, the merge and the right
  // sibling of the merge. If we are able to merge, we choose a merge page to
  // merge that page into the left sibling.
  //
  // We now descend the tree with the key for the merge page. We lock that page
  // exclusively. We go left then right to the level of the left sibling. We go
  // right then left to reach the merge page. (We're using page addresses to
  // know that we've reached the merge page, the key is going to only be useful
  // to find path on takes to find the left sibling.) We can then copy append
  // addresses to the left sibling. Remove the merge sibling from its parent.
  // Delete the key from where we found it, so it can be looked up again.
  //
  // Before we lose track of the sub&#x2011;tree we're in, we descend to the
  // potentially new left most leaf of the parent, and obtain its key to repeat
  // the process.
  //
  // #### Filling the Root Page
  //
  // If the parent is the root page, we only do something special when the root
  // page reaches one child and no keys. At that point, the one child becomes
  // the root. We will have deleted the last key, merged everything to the left.
  //
  // We descend again, locking the root. We lock the one child of the root. We
  // copy the contents of the one root child into the root and delete the child.
  //
  // This will decrease the height of the b&#x2011;tree by one.
  //
  // #### Deleting Pages
  //
  // A page deletion is simply a merge where we prefer to use the empty page as
  // the merge page. We have to make an exception when the empty page is the
  // left most page in the entire tree, which is not uncommon for time series
  // data where the oldest records are regularly purged.
  //
  // #### Purging Deleted First Keys
  //
  // Now we merge the parents. We find a penultimate page by the key of the left
  // most leaf. Similar go left, then get the up to three pages. See if they
  // will fit. Keep them around in cache. You may visit them again soon.
  //
  // If they fit, then you merge them. Lock exclusively the form page where you
  // found the key. Move left into the right. Rewriting. Remove the key from the
  // parent. You will have it locked. Delete the key.
  //
  // Then, get the left most leaf key. Okay there is only one thread balancing,
  // so we will have a consistent depth. This is merge.
  //
  // Delete the key to trigger the lookup.
  //
  // #### Deleted First Keys
  //
  // We fix deleted first keys at balance. Descend locking the key when we see
  // it. Mark it deleted forever. Then delete the key. It will get looked up
  // again.
  //
  // We split recursively. Split and put into the parent. If the parent is ready
  // to split, descend the tree locking the parent of the parent exclusively.
  // We'll track where that is. No one else is changing the height of the tree.
  // Only one thread is changing the height of the tree.
  //
  // We will create a plan to merge and split. We execute our plan. If we reach
  // a place where our plan is invalid, we requeue, but only if it is invalid.
  // If it is not invalid, only different, we continue with our merge, or our
  // split.
  //
  // This give us a potential for live lock. We're constantly creating plans to
  // merge, that are invalidated, so we create plans and those are invalidated.
  //
  // Our descent of the tree to balance will be evented anyway. We can probably
  // make our calculation evented. We were fretting that we'd exacerbate the
  // live lock problem. Live lock is a problem if it is a problem. There is no
  // real gauntlet to run. The user can determine if balance will live lock. If
  // there are a great many operations, then balance, wait a while, then
  // balance, wait a while. It is up the end user.
  //
  // The end user can use the b&#x2011;tree a map, tucking in values, getting
  // them out. Or else, as an index, to scan, perform table scans. We'll figure
  // that out.
  //
  // Now I have an API problem. The client will have to know about pages to work
  // with them. We can iterate through them, in a table scan. We can implement a
  // merge. We probably need an intelligent cursor, or a reporting cursor.

  // There has been a desire second guess the most-recently used list. There is
  // a desire to link nodes to the back of the list or suspend purges.
  //
  // There is a desire to cache the addresses of the left sibling pages when
  // possible, so I wouldn't have to descend the tree. Except that the
  // most-recently used list works hand in hand with b&#x2011;tree descent. The
  // higher levels of the tree are kept in memory, because they are more
  // frequently visited than lower levels. To much iteration along one level
  // threatens to purge other levels.
  //
  // One can imagine that when balancing b&#x2011;tree that has been left
  // unbalanced for a long time, reading in many unbalanced leaf pages will
  // cause the first ones to flush, which is a pity, since we'll probably need
  // one of them.
  //
  // Perhaps we suspect a page needs to be split but it doesn't. If the balancer
  // was the one to load the page, simply to determine that nothing needs to be
  // done, their is a desire to expedite the removal of the page from the cache.
  //
  // There are so many desires. It makes one giddy with thoughts of premature
  // optimization.
  //
  // We're going to descent the tree to find our left sibling to exercise the
  // most-recently used cache. We are not going to second guess it. We're going
  // to defer to the algorithms. The simpler the code, the more trust you can
  // have in the code, the more likely your code will be adopted. A wide user
  // base can inform decisions on optimization. There is always a core of what
  // your application needs to do, and Strata needs to search and edit records.
  //
  // Balancing the tree is maintenance. The balancer can take its time.

  function Balancer () {
    var lengths = {}
      , operations = []
      , referenced = {}
      , ordered = {}
      , ghosts = {}
      , methods = {}
      ;

    classify.call(methods, deleteGhost, splitLeaf, mergeLeaves);

    function balancerReport () {
      return extend(report(), {
        referenced: Object.keys(referenced),
        lengths: extend({}, lengths)
      });
    }

    // Mark a page as having been altered, now requiring a test for balance. If
    // the `force` flag is set, the value is set to the leaf order, so that if
    // the record count of the page is less than the order of the leaf page, it
    // will be test for merge. If it is greater than the order of the leaf page,
    // it will be split. Of course, if it the order of the page, it can not be
    // merged, nor should it be split.
    function unbalanced (page, force) {
      if (force) {
        lengths[page.address] = options.leafSize;
      } else if (lengths[page.address] == null) {
        lengths[page.address] = page.positions.length - page.ghosts;
      }
    }

    // **TODO**: You will have to launch this in a worker thread, otherwise
    // there is no good way for you to handle the error, or rather, you're going
    // to have to have some form of error callback, which is a pattern I'd like
    // to avoid.

    // **TODO**: Uh, no. You can kind of launch this whenever the who, so long
    // as you do not launch more than one at a time. Use a callback. The
    // callback can record your errors to an error log. Do note that balance is
    // always concurrent, though. Makes no sense to try to run more than one at
    // a time, or it doesn't make sense to run a balance when a balance is
    // running, or rather, it doesn't make sense to make it a matter of running
    // one after each insert. Horrible writing here. Do not use.

    // **TODO**: Once loaded, and marked as part of the balancer, we can do our
    // calculations in one fell swoop. This triggers the consternation over what
    // all these extraneous pages do to the cache.

    // **TODO**: What is the procedure for vacuuming deleted keys? &mdash; We
    // check every page that has been added to the balancer, regardless of
    // whether it has grown, shrunk or returned to its original reported size.
    // If the page is to be deleted, because the leaf page is empty, that
    // negates any fussing with the key. Same goes for the case where the page
    // is to be merged.

    // Ah, also, when we do load these, when we want to get them from the cache,
    // we don't really need them to be loaded. We should reach in a probe the
    // cache ourselves. My former Java self would have to spend three days
    // thinking about encapsulation, probably create a new sub-project. Agony.

    // Balancing will continue until morale improves. It may feel like a race
    // condition, but that can't be helped. There may be degenerate use cases
    // where the b&#x2011;tree cannot reach a balanced state. Inserts and
    // deletes may be taking a set of pages from needing split to needing merge
    // faster than the balance plan can figure it, and the balance operations
    // can make it so.

    // The balance plan is optimistic. It creates a plan based on the state of
    // the tree a specific point in time. While implementing the plan, however,
    // the state of the tree may change, invalidating aspects of the plan. In
    // this case, an operation will be canceled. When an operation is canceled,
    // we add the canceled pages to the next balancer.

    // **TODO**: Tracking the difference means we can short cut planning, if the
    // page has only grown. This is a short cut. We consider its use carefully.
    // We are not capricious with it. We are okay with having to load the
    // sibling page lengths to check for merge. A split will lead to a
    // subsequent balance plan that will load four pages. In that regard, splits
    // are not cheaper than merges.

    // No. Race condition. We want to gather all the pages in memory, so we can
    // evaluate them, without someone else changing them. We add a page because
    // it has grown, but then, when we imagine that we've gathered all the pages
    // necessary, it turns out that in the mean time, that page has shrunk so
    // that it is now too small. We could create an outer loop that keeps on
    // referencing cache entries until all are available, but then you have an
    // outer strange condition where you might load the entire b&#x2011;tree,
    // because you're taking so long, and every page is being touched. Allow
    // balance to be imperfect.
    function balance (callback) {
      var check = validator(callback), address;

      // We only ever run one balance at a time. If there is a balance in
      // progress, we do not proceed.
      if (balancing) return callback(null);

      // We do not proceed if there is nothing to consider.
      var addresses = Object.keys(lengths);
      if (addresses.length == 0) {
        callback(null);
      // Otherwise we put a new balancer in place of the current balancer. Any
      // edits will be considered by the next balancer.
      } else {
        balancer = new Balancer();
        balancing = true;
        gather();
      }

      // Prior to calculating a balance plan, we gather the sizes of each leaf
      // page into memory. We can then make a balance plan based on page sizes
      // that will not change while we are considering them in our plan.
      //
      // However, page size may change between gathering and planning, and page
      // size may change between planning and executing the plan. Staggering
      // gathering, planning and executing the balance gives us the ability to
      // detect the changes in page size. When we detect that we can't make an
      // informed decision on a page, we pass it onto the next balancer for
      // consideration at the next balance.

      // For each page that has changed we add it to a doubly linked list.

      //
      function gather () {
        // Convert the address back to an integer.
        var address = +(addresses.shift()), length = lengths[address], right, node;

        // We create linked lists that contain the leaf pages we're considering
        // in our balance plan. This is apart from the most-recently used list
        // that the pages themselves form.
        //
        // The linked list nodes contain a reference to the page, plus a
        // reference to the node containing the previous sibling, and a
        // reference to the node containing the next sibling. If a sibling is
        // not participating in our balance plan, then its link is null. This
        // gives us one or more linked lists that reference a series of leaf
        // pages ordered according to their order in the b&#x2011;tree.
        //
        // We are always allowed to get a lock on a single page, so long as
        // we're holding no other locks.
        if (node = ordered[address]) checkMerge(node);
        else lock(address, false, nodify(checkMerge));

        // Build a callback function that will add a leaf page to our collection
        // of gathered pages, then invoke the `next` function passing the
        // balance list node. The leaf page must be locked. The function will
        // mark the page as being a participant in a balance, then unlock it.
        // Linking to sibling nodes is not performed here.
        function nodify (next) {
          return check(function (page) {
            ok(page.address % 2, "leaf page expected");
            if (page.address == 1) identified();
            else designate(page, 0, check(identified));
            function identified (key) {
              node = { key: key,
                       address: page.address,
                       rightAddress: page.right,
                       length: page.positions.length - page.ghosts };
              unlock(page);
              ordered[node.address] = node
              if (page.ghosts)
                ghosts[node.address] = node
              check(function () { next(node) }, "reference", balancerReport)(null);
            }
          });
        }

        // If the page has shrunk in size, we gather the size of the left
        // sibling page and the right sibling page to check to see if the
        // page can be merged with its siblings.
        //
        // If it has not shrunk in size, we assume that the page will not merge
        // with its siblings because if it could have merged with its siblings,
        // it would have been merged by a previous balance. We will check the
        // page for a split, but not a merge.
        function checkMerge(node) {
          if (node.length - length < 0) {
            if (node.address != 1 && ! node.left) leftSibling(node);
            else rightSibling(node);
          } else {
            next();
          }
        }

        function leftSibling (node) {
          var descent = new Descent();
          descent.descend(descent.key(node.key), descent.found([node.key]), check(goToLeaf));

          function goToLeaf () {
            descent.index--;
            descent.descend(descent.right, descent.leaf, check(checkLists));
          }

          // **FIXME**: Does the cache hit path release the lock on the descent?
          // I don't believe so.
          function checkLists () {
            var left;
            if (left = ordered[descent.page.address]) {
              unlock(descent.page);
              attach(left);
            } else {
              nodify(attach)(null, descent.page);
            }
          }

          function attach (left) {
            left.right = node
            node.left = left

            rightSibling(node);
          }
        }

        // Link the right sibling to examine for a possible merge.
        function rightSibling (node) {
          var right;

          if (!node.right && node.rightAddress)  {
            if (right = ordered[node.rightAddress]) attach(right);
            else lock(node.rightAddress, false, nodify(attach));
          } else {
            next();
          }

          function attach (right) {
            node.right = right
            right.left = node

            next();
          }
        }

        function next () {
          if (addresses.length) {
            gather();
          } else {
            check(function () { plan(callback) }, "plan", balancerReport)(null);
          }
        }
      }
    }

    // The remainder of the calculations will not be interrupted by evented I/O.
    // Gather the current counts of each page into the node itself, so we adjust
    // the count based on the merges we schedule.
    function plan (callback) {
      var address, node, difference, addresses;

      // Calculate the actual length of the page less ghosts.
      for (address in ordered) {
        node = ordered[address];
      }

      // Break the link to next right node and return it.
      function terminate (node) {
        var right;
        if (node) {
          if (right = node.right) {
            node.right = null
            right.left = null
          }
        }
        return right;
      }

      // Unlink a node.
      function unlink (node) {
        terminate(node.left);
        terminate(node);
        return node;
      }

      // Break the lists on the nodes that we plucked because we expected that
      // they would split. Check to see that they didn't go from growing to
      // shrinking while we were waiting evented I/O. Note how we drop the page
      // if a split is not necessary.

      //
      for (address in lengths) {
        length = lengths[address];
        node = ordered[address];
        difference = node.length - length;
        // If we've grown past capacity, split the leaf. Remove this page from
        // its list, because we know it cannot merge with its neighbors.
        if (difference > 0 && node.length > options.leafSize) {
          // Schedule the split.
          operations.unshift({  method: "splitLeaf", parameters: [ node.key, ghosts[node.address] ] });
          // If there's a ghost, we call `deleteGhost` from within `splitLeaf`.
          delete ghosts[node.address];
          // Unlink this split node, so that we don't consider it when merging.
          unlink(node);
        }
      }

      // Ordered now becomes a map of the heads of lists of leaf pages that are
      // candidates for merge. If a page in the ordered map has a left sibling
      // it is removed from the ordered map because it is linked to the ordered
      // map though it's left sibling.
      for (address in ordered) {
        if (ordered[address].left) delete ordered[address];
      }

      // Now we break the links between pages that cannot merge, pair up the
      // pages that can merge. We only merge two leaf pages at a time, even when
      // we could combine more than two to file a leaf page. **FIXME**: Not so.
      for (address in ordered) {
        var node = ordered[address];
        while (node && node.right) {
          if (node.length + node.right.length > options.leafSize) {
            node = terminate(node);
            ordered[node.address] = node;
          } else {
            if (node = terminate(node.right)) {
              ordered[node.address] = node;
            }
          }
        }
      }

      // Merge the node to the right of each head node into the head node.
      for (address in ordered) {
        node = ordered[address];
        // Schedule the merge. Ghost tidy is performed by the merge. We cannot
        // use the standard delete ghost operation because the left page of the
        // merge may be empty.
        //
        // If it is empty, once we delete the ghost, there is no record to use
        // for a leaf page key. The only way to deal with empty pages is to
        // merge them into their right siblings.
        //
        // Which creates another special case, merging two empty pages. If the
        // left page is not the left most page, it is going to need a key, so it
        // may as well keep the ghost record and it's key. Again, the default
        // delete ghosts has no concept of this.

        //
        if (node.right) {
          ok(!node.right.right, "merge pair still linked to sibling");
          operations.unshift({
            method: "mergeLeaves",
            parameters: [ node.right.key, node.key, lengths, !!ghosts[node.address] ]
          });
          delete ghosts[node.address];
          delete ghosts[node.right.address];
        }
      }

      // Rewrite position arrays to remove ghosts.
      for (address in ghosts) {
        node = ghosts[address];
        if (node.length) operations.unshift({
          method: "deleteGhost",
          parameters: [ node.key ]
        });
      }

      operate(callback);
    }

    function operate (callback) {
      var check = validator(callback), address;
      function shift () {
        var operation = operations.shift();
        if (operation) {
          // Maybe use bind instead.
          methods[operation.method].apply(this, operation.parameters.concat(check(shift)));
        } else {
          // Allow a subsequent balance.
          balancing = false;
          callback(null);
        }
      }
      shift();
    }

    // ### Should We Split a Branch?
    //
    // Thank goodness for Streamline.js. We can recursively call split to split
    // our branch pages, if they need to be split.
    //
    // We call this method with unlocked branch pages. That's okay, because only
    // the balance can alter a branch page. Even if the unlocked branch page is
    // purged from the cache, and subsequently reloaded, the address and length
    // of the page it represents will not change.

    // &mdash;
    function shouldSplitBranch (branch, key, callback) {
      // Are we larger than a branch page ought to be?
      if (branch.addresses.length > options.branchSize) {
        // Either drain the root, or split the branch.
        if (branch.address == 0) {
          drainRoot(callback);
        } else {
          splitBranch(branch.address, key, callback);
        }
      } else {
        callback(null);
      }
    }
    // **TODO**: What if the leaf has a deleted key record? Abandon. We need to
    // have purged deleted key records before we get here. For example, it may
    // be the case that a leaf page key has been deleted, requiring a page key
    // swap. The page might also need to be split. We push the split onto the
    // next balance. (Bad example, we really don't have to do this.)
    //
    // **TODO**: When added to the balancer, we note the size of the leaf page
    // when it was last known to be balanced in relation to its siblings. Until
    // we can either delete it or run it through a plan where it is known to be
    // balanced, it is in an unbalanced state.
    //
    // **TK**: Docco.

    //
    function splitLeaf (key, ghosts, callback) {
      // Keep track of our descents so we can unlock the pages at exit.
      var check = validator(callback)
        , descents = []
        , replacements = []
        , uncached = []
        , completed = 0
        , penultimate, leaf, split, pages, page
        , records, remainder, right, index, offset, length
        ;

      // todo: check again for ghosts, they may have appeared in the time since
      // we created our plan, in fact, why are we not doing a separate search
      // and descent here?

      // todo: Wait a tick! What is the key if the page is the 1st page?
      if (ghosts) deleteGhost(key, check(exorcised));
      else penultimate();

      function exorcised (rekey) {
        key = rekey;
        penultimate();
      }

      function penultimate () {
        // We descend the tree directory directly to the leaf using the key.
        descents.push(penultimate = new Descent());

        // Descend to the penultimate branch page, from which a leaf page child
        // will be removed.
        penultimate.descend(penultimate.key(key), penultimate.penultimate, check(upgrade));
      }

      // Upgrade to an exclusive lock.
      function upgrade () {
        penultimate.upgrade(check(fork));
      }

      // Now descend to our leaf to split.
      function fork () {
        descents.push(leaf = penultimate.fork());
        leaf.descend(leaf.key(key), leaf.leaf, check(dirty));
      }

      // If it turns out that our leaf has drained to the point where it does
      // not need to be split, we should then check to see if it can be merged.
      function dirty () {
        // **TODO**: We're not bothering with split when we've only grown a bit,
        // right?
        split = leaf.page;

        if (split.positions.length - split.ghosts <= options.leafSize) {
          balancer.unbalanced(split, true);
          cleanup();
        } else {
          // Otherwise we perform our split.
          partition();
        }
      }

      function partition () {
        // It may have been some time since we've split, so we might have to
        // split into more than two pages.
        pages = Math.ceil(split.positions.length / options.leafSize);
        records = Math.floor(split.positions.length / pages);
        remainder = split.positions.length % pages;

        right = split.right

        // Never a remainder record on the first page.
        offset = split.positions.length

        paginate();
      }

      function paginate () {
        if (--pages) shuffle();
        else paginated();
      }

      // Create a new page with some of the children of the split page.
      function shuffle () {
        // Create a new leaf page.
        page = createLeaf({ loaded: true });

        // Link the leaf page to its siblings.
        page.right = right;
        right = page.address;

        // Add the address to our parent penultimate branch.
        splice('addresses', penultimate.page, penultimate.index + 1, 0, page.address);

        // Determine the number of records to add to this page from the split
        // leaf. Add an additional record if we have a remainder.
        length = remainder-- > 0 ? records + 1 : records;
        offset = split.positions.length - length;
        index = offset;

        copy();
      }

      function copy () {
        // Fetch the record and read it from cache or file.
        var position = split.positions[index];

        ok(index < split.positions.length);

        stash(split, index, check(uncache));

        function uncache (object) {
          uncacheRecord(split, position);
          // Add it to our new page.
          splice('positions', page, page.positions.length, 0, position);
          splice('lengths', page, page.lengths.length, 0, split.lengths[index]);
          cacheRecord(page, position, object.record, object.key);
          index++;
          if (index < offset + length) copy();
          else copied();
        }
      }

      // We've copied records from one leaf to another. Now we need to write out
      // the new leaf.
      function copied() {
        // Remove the positions that have been merged.
        splice('positions', split, offset, length);
        splice('lengths', split, offset, length);

        page.key = page.cache[page.positions[0]].key

        // Schedule the page for replacing and encaching.
        replacements.push(page);
        uncached.push(page);

        // Write the left most leaf page from which new pages were split.
        rewriteLeaf(page, ".replace", check(replaced));
      }

      function replaced () {
        paginate();
      }

      // Write the penultimate branch.
      function paginated () {
        // Link the leaf page to the last created new leaf page.
        split.right = right;

        if (split.address == 1) rewrite([]);
        else designate(split, 0, check(function (key) { rewrite([key]) }));

        function rewrite (key) {
          // Write the left most leaf page from which new pages were split.
          rewriteLeaf(split, ".replace", check(transact));

          // Schedule the page for replacing and encaching.
          replacements.push(split);
        }
      }

      // Write the penultimate branch.
      function transact () {
        writeBranch(penultimate.page, ".pending", check(commit, "splitLeafCommit", report));
      }

      // Now rename the last action, committing to our balance.
      function commit () {
        rename(penultimate.page, ".pending", ".commit", check(persist));
      }

      // Rename our files to put them in their place.
      function persist () {
        replacements.forEach(function (page) { replace(page, ".replace", check(complete)) });
      }

      function complete (callback) {
        if (++completed == replacements.length) {
          // Add our new pages to the cache.
          uncached.forEach(function (page) { encache(page) } );

          // This last replacement will complete the transaction.
          replace(penultimate.page, ".commit", check(rebalance));
        }
      }

      // Our left-most and right-most page might be able to merge with the left
      // and right siblings of the page we've just split. We compel a merge
      // detection in the next balance plan by setting the last known size. We
      // do not use the current size, because it is not **known** to be
      // balanced. We cannot employ the split shortcut that only checks for
      // split if a page has grown from being known to be balanced with
      // siblings. Sorry, English bad, but great example. Imagine a page that
      // has been full, but has a sibling that has only one record. We add a
      // record to the full page and split it so that it is half empty. We then
      // add it to the balancer with its half full record count. We want to
      // check for merge and see
      //
      // **TODO**: Swipe &mdash; This always balance until perfect balance is
      // still imperfect.  We may still manage to create a b&#x2011;tree that
      // has leaf pages that alternate from full pages to pages containing a
      // single record, a degenerate case.

      //
      function rebalance () {
        balancer.unbalanced(leaf.page, true);
        balancer.unbalanced(page, true);

        cleanup();
      }

      function cleanup() {
        // Release the pages locked during descent. Seems premature because
        // we're not done yet, but no other descent makes progress unless we
        // invoke a callback.
        descents.forEach(function (descent) { unlock(descent.page) });

        // Although we've unlocked the penultimate branch page, we know that
        // only the balancer edit a branch page, so we are safe to make a
        // decision about whether to split the penultimate branch page while it
        // is unlocked.
        shouldSplitBranch(penultimate.page, key, callback);
      }
    }

    // &mdash;
    function splitBranch (address, key, callback) {
      // Keep track of our descents so we can unlock the pages at exit.
      var check = validator(callback)
        , descents = []
        , children = []
        , parent, full, split, pages
        , records, remainder, offset
        ;

      // We descend the tree directory directly to the leaf using the key
      // stopping when we find the parent branch page of the branch page we want
      // to split.
      descents.push(parent = new Descent());

      // Descend to the penultimate branch page, from which a leaf page child
      // will be removed.
      parent.descend(parent.key(key), parent.child(address), check(upgrade));

      // Upgrade to an exclusive lock.
      function upgrade () {
        parent.upgrade(check(fork));
      }

      // Now descend to our leaf to split.
      function fork () {
        descents.push(full = parent.fork());
        full.descend(full.key(key), full.level(full.depth + 1), check(partition));
      }

      // Unlike the leaf page, we do not have to reassure ourselves that the
      // page needs to be split because the size of a branch page is only
      // affected by the balancer thread.

      // Split the branch.
      function partition () {
        split = full.page;

        // It may have been some time since we've split, so we might have to
        // split into more than two pages.
        pages = Math.ceil(split.addresses.length / options.branchSize);
        records = Math.floor(split.addresses.length / pages);
        remainder = split.addresses.length % pages;

        // Never a remainder record on the first page.
        offset = split.addresses.length

        paginate();
      }

      function paginate () {
        // Create a new branch page.
        var page = createBranch({});

        // Add the branch page to our list of new child branch pages.
        children.push(page);

        // Determine the number of records to move from the root branch into the
        // new child branch page. Add an additional record if we have a
        // remainder.
        var length = remainder-- > 0 ? records + 1 : records;
        var offset = split.addresses.length - length;

        // Cut off a chunk of addresses.
        var cut = splice('addresses', split, offset, length);

        // Uncache the keys from our splitting branch.
        cut.forEach(function (address) { uncacheKey(split, address) });

        // Add the keys to our new branch page.
        splice('addresses', page, 0, 0, cut);

        // Continue until there is one page left.
        if (--pages > 1) paginate();
        else paginated();
      }

      // Write the penultimate branch.
      function paginated () {
        // Get our children in the right order. We were pushing above.
        children.reverse()

        // Insert the child branch page addresses onto our parent.
        splice('addresses', parent.page, parent.index + 1, 0, children.map(function (page) { return page.address }));

        // Add the split page to the list of children, order doesn't matter.
        children.unshift(full.page);

        // Write the child branch pages.
        children.forEach(function (page) { writeBranch(page, ".replace", check(childWritten)) });
      }

      var childrenWritten = 0;

      // Rewrite our parent.
      function childWritten () {
        if (++childrenWritten == children.length) {
          writeBranch(parent.page, ".pending", check(rootWritten));
        }
      }

      // Commit the changes.
      function rootWritten () {
        rename(parent.page, ".pending", ".commit", check(committing));
      }

      // Write the child branch pages.
      function committing () {
        children.forEach(function (page) { replace(page, ".replace", check(childCommitted)) });
      }

      var childrenCommitted = 0;

      // Commit complete.
      function childCommitted (callback) {
        if (++childrenCommitted == children.length) {
          replace(parent.page, ".commit", check(cleanup));
        }
      }

      // **TODO**: Our left-most and right-most page might be able to merge with
      // the left and right siblings of the page we've just split. We deal with
      // this in split leaf, but not here in split branch as of yet.

      // &mdash;
      function cleanup() {
        // Release the pages locked during descent. Seems premature because
        // we're not done yet, but no other descent makes progress unless we
        // invoke a callback.
        descents.forEach(function (descent) { unlock(descent.page) });

        // Although we've unlocked the parent branch page, we know that only the
        // balancer edit a branch page, so we are safe to make a decision about
        // whether to split the penultimate branch page while it is unlocked.
        shouldSplitBranch(parent.page, key, callback);
      }
    }

    // ### Drain Root
    //
    // When the root branch page is full we don't split it so much as we drain
    // it. We copy the child pages of the root branch page into new branch
    // pages. The new branch pages become the new child pages of the root branch
    // page.
    //
    // This balance operation will increase the height of the b&#x2011;tree. It
    // is the only operation that will increase the height of the b&#x2011;tree.

    // &mdash;
    function drainRoot (callback) {
      var root, pages, records, remainder, children = [], check = validator(callback);

      // Lock the root. No descent needed.
      lock(0, true, check(partition));

      function partition ($root) {
        root = $root;
        // It may have been some time since we've split, so we might have to
        // split into more than two pages.
        pages = Math.ceil(root.addresses.length / options.branchSize);
        records = Math.floor(root.addresses.length / pages);
        remainder = root.addresses.length % pages;

        paginate();
      }

      function paginate () {
        // Create a new branch page.
        var page = createBranch({});

        // Add the branch page to our list of new child branch pages.
        children.push(page);

        // Determine the number of records to move from the root branch into the
        // new child branch page. Add an additional record if we have a
        // remainder.
        var length = remainder-- > 0 ? records + 1 : records;
        var offset = root.addresses.length - length;

        // Cut off a chunk of addresses.
        var cut = splice('addresses', root, offset, length);

        // Uncache the keys from the root branch.
        cut.forEach(function (address) { uncacheKey(root, address) });

        // Add the keys to our new branch page.
        splice('addresses', page, 0, 0, cut);

        // Continue until all the pages have been moved to new pages.
        if (--pages) paginate();
        else paginated();
      }

      function paginated () {
        // Get our children in the right order. We were pushing above.
        children.reverse()

        // Push the child branch page addresses onto our empty root.
        splice('addresses', root, 0, 0, children.map(function (page) { return page.address }));

        // Write the child branch pages.
        children.forEach(function (page) { writeBranch(page, ".replace", check(childWritten)) });
      }

      var childrenWritten = 0;

      // Rewrite our root.
      function childWritten () {
        if (++childrenWritten == children.length) {
          writeBranch(root, ".pending", check(rootWritten));
        }
      }

      // Commit the changes.
      function rootWritten () {
        rename(root, ".pending", ".commit", check(committing));
      }

      // Write the child branch pages.
      function committing () {
        children.forEach(function (page) { replace(page, ".replace", check(childCommitted)) });
      }

      var childrenCommitted = 0;

      // Commit complete.
      function childCommitted (callback) {
        if (++childrenCommitted == children.length) {
          replace(root, ".commit", check(rootCommitted));
        }
      }

      // Add our new children to the cache.
      function rootCommitted () {
        children.forEach(function (page) { encache(page) });
        // Release our lock on the root.
        unlock(root);
        // Do we need to split the root again?
        if (root.addresses.length > options.branchSize) drainRoot(callback);
        else callback(null);
      }
    }

    // Remove a ghost record from the positions array of the leaf page
    // `ghostly`. When the ghost is removed, update the key in the branch page
    // referenced by the `pivot` with the first key of the key of the leaf page
    // `corporal`, excluding it's possible ghost. During split and ghost
    // deletion, `ghostly` and `corporal` are the same, during merge they are
    // different if the left page of the merge is empty. If the left page of the
    // merge is empty, the key will come from the right page, so the left page
    // is `ghostly` and the right page is `corporal`.
    //
    // Returns the promoted key.

    //
    function exorcise (pivot, ghostly, corporal, callback) {
      var fd, check = validator(callback);

      // Shouldn't call unless necessary.
      ok(ghostly.ghosts, "no ghosts");
      ok(corporal.positions.length - corporal.ghosts > 0, "no replacement");

      // Remove the ghosted record from the references array and the record cache.
      uncacheRecord(ghostly, splice('positions', ghostly, 0, 1).shift());
      splice('lengths', ghostly, 0, 1);
      ghostly.ghosts = 0

      // Open the leaf page file and write out the shifted positions array.
      fs.open(filename(ghostly.address), 'a', 0644, check(leafOpened));

      function leafOpened (fd) {
        writePositions(fd, ghostly, check(written));

        function written () {
          fs.close(fd, check(closed));
        }

        function closed () {
          stash(corporal, corporal.ghosts, check(rekey));
        }
      }

      // Note that we're assigning the page key now, but in the case of a leaf
      // page merge with an empty left leaf page, the cache and positions are
      // going to be updated by the merge, not by this function.
      function rekey (entry) {
        cacheKey(pivot.page, pivot.page.addresses[pivot.index], entry.key);
        callback(null, ghostly.key = entry.key);
      }
    }

    // We delete ghosts as part of a balance; we replace the key in branch with
    // the first non-ghosted record, then we rewrite the positions array.

    //
    function deleteGhost (key, callback) {
      var descents = [], pivot, leaf, fd, check = validator(callback);

      // Descend to the ghost key and lock exclusively.
      descents.push(pivot = new Descent());
      pivot.descend(pivot.key(key), pivot.found([key]), check(upgrade));

      function upgrade () {
        pivot.upgrade(check(descendLeaf));
      }

      // Lock the pivot key. We're going to write a new key to the branch page
      // log and replace the key in the cache.
      //
      // todo: uncaching is outgoing.
      function descendLeaf () {
        descents.push(leaf = pivot.fork());
        leaf.uncaching = true;

        leaf.descend(leaf.key(key), leaf.leaf, check(shift));
      }

      // Remove the ghosted record from the references array and the record cache.
      function shift () {
        exorcise(pivot, leaf.page, leaf.page, check(release));
      }

      // Release all locks.
      function release (key) {
        descents.forEach(function (descent) { unlock(descent.page) });
        callback(null, key);
      }
    }

    // A generalized merge function with a specialization each for branch pages
    // and leaf pages below. The function merge a page into its left sibling. A
    // specialized implementation of a merge invokes this function providing
    //
    //  * a key that designates the page to merge into it's left sibling,
    //  * optionally, the key of the left sibling if it needs to be locked,
    //  * a stopper function that will generate a stop function that will stop
    //  the descent at the page to merge,
    //  * a merger function that will accept the two pages to merge as
    //  arguments, merge them, and write out the left page, and
    //  * a callback to invoke when the merge is completed.

    function mergePages (key, leftKey, stopper, merger, ghostly, callback) {
      // Create a list of descents whose pages we'll unlock before we leave.
      var check = validator(callback),
          descents = [], locked = [], singles = [], parents = {}, pages = {},
          ancestor, pivot, empties, ghosted, designation;

      var keys = [ key ]
      if (leftKey) keys.push(leftKey)

      // Descent the tree stopping at the branch page that contains a key for
      // the leaf page that we are going to delete when we merge it into its
      // left sibling. We're going to refer to this branch page as the pivot
      // branch page in the rest of this function. We need to uncache the key
      // from the pivot branch page, because it will no longer be correct once
      // the leaf page is deleted.
      descents.push(pivot = new Descent());
      pivot.descend(pivot.key(key), pivot.found(keys), check(lockPivot))

      // Get an exclusive lock on the branch page that contains the right key
      // or, if we're merging branch pages, the right key or the left key, which
      // ever comes first. After upgrade our shared lock to an exclusive lock,
      // all locks as we descend the tree will be exclusive.
      function lockPivot () {
        var found = pivot.page.cache[pivot.page.addresses[pivot.index]]
        if (comparator(found, keys[0]) == 0) {
          pivot.upgrade(check(atPivot));
        } else {
          pivot.upgrade(check(leftAboveRight));
        }
      }

      // If we need to lock the left key for ghost busting, and the left key is
      // above the right key, we need to capture the left key as the ghosted
      // descent, then descend to right key. We've already upgrade our descent
      // so that it will exclusively.
      function leftAboveRight () {
        descents.push(pivot = (ghosted = pivot).fork())
        keys.pop()
        pivot.uncaching = true
        pivot.descend(pivot.key(key), pivot.found(keys), check(atPivot))
      }

      // We're at the pivot, the branch containing the right key. The path to
      // the left of the right key is the path to left branch of the merge,
      // provided you go down the left path once, and then all the way to the
      // right. From the pivot, descend to the branch page that is the parent of
      // the page for the given key.
      function atPivot () {
        // If we have a left key, but we've yet to encounter a ranch page that
        // contains the left key, then guess what; it's this page. Fork the
        // current descent, decrement it's index and make not of it as the
        // ghosted branch.
        if (leftKey && !ghosted) {
          descents.push(ghosted = pivot.fork());
          ghosted.index--;
        }

        // Create a forked descent that will descend to the parent branch page
        // of the right page of the merge.
        parents.right = pivot.fork();

        // We gather branch pages on the path to the right page of the merge
        // with a single child whose descendants consist only of the leaf page
        // or a branch page with a single child. These are pages that must be
        // deleted. Basically any direct ancestors of the right leaf page with
        // only one child form a linked list to the leaf page. After the right
        // page is merged into its left sibling and deleted, the linked list
        // goes nowhere.
        //
        // Another way of thinking of it is that we a branch page with only one
        // child doesn't count as a parent for the sake of merge. We need to
        // delete it and remove an entry from it's parent branch page. Unless,
        // of course, it's parent branch page has only one child, etc.
        //
        // If we are gathering pages and encounter a branch page with more than
        // one child, we release the pages we've gathered.
        parents.right.unlocker = function (parent, child) {
          if (child.addresses.length == 1) {
            if (singles.length == 0) singles.push(parent);
            singles.push(child);
          } else if (singles.length) {
            if (singles[0].address == pivot.page.address) singles.shift();
            singles.forEach(unlock);
            singles.length = 0;
          } else if (parent.address != pivot.page.address) {
            unlock(parent);
          }
        }

        // Descent to the parent branch page of the right page of the merge.
        parents.right.descend(parents.right.key(key), stopper(parents.right), check(atRightParent));
      }

      // Gerk! Gerk! Gerk! Gerk alert! Gerk level: triple gerk.
      //
      // I'm trying to sort out how to descent to the left page, while trying to
      // sort out how to hold a lock on the left key pivot, if we're merging
      // leaf pages. I've become confused, hence the gerk alert, and I do hope
      // that gerk doesn't mean anything profane to someone in my far flung
      // audience, it's the noise I made when I realized how stupid I am.
      //
      // Anyhoo, all this writing is getting old and needs to be refreshed, I'm
      // dreading it. Some humor to blunt the dread. Also, to see where the
      // Docco is rotting, like right here.
      //
      // Somewhere in this Docco, it has probably already been said, but I'll
      // say it again, we use the key of the right leaf page, because we know it
      // exists in the page. When we find it, we know that the child that
      // precedes the right key is the path to the left branch. There is no
      // other way. Introducing the locking of the left key for ghost busting,
      // that confused me. It might confuse you as well. I need to lock the page
      // that contains the key, but that doesn't change the logic to go down the
      // path to the left page. I don't want to change too much logic. It's
      // pretty stable and somewhat tested.
      //
      // From the pivot, descent to the parent branch page of the left sibling
      // of page for the given key.
      //
      // Note that while we must lock leaf pages from left to right, we can lock
      // branch pages right to left because the balance descent is the only
      // descent that will lock two sibling branch pages at the same time,
      // search and mutate descents will only follow a single ancestral path
      // down the tree. Once a search or mutate descent reaches a leaf page, it
      // then has the option to navigate from leaf page to leaf page from left
      // to right.
      function atRightParent () {
        parents.left = pivot.fork();
        parents.left.index--;
        // todo: that ghosted page up there, we could just use the left parent
        // here, not an additional descent. But, meh.
        parents.left.descend(parents.left.right,
                             parents.left.level(parents.right.depth),
                             check(atLeftParent));
      }

      // Take note of which pages have been locked during our descent to find
      // the parent branch pages of the two pages we want to merge.
      function atLeftParent (callback) {
        // If we encountered singles, then we're going to delete all the branch
        // pages that would be empty, then remove a child from an ancestor above
        // the parent branch page of the page we're merging.
        if (singles.length) {
          locked = singles.slice();
          ancestor = singles.shift();
        // If we encountered no singles, then we may still have gone down
        // separate paths to reach the parent page. If so, the pivot branch page
        // and the parent branch page are separate, so we can
        } else {
          ancestor = parents.right.page;
          if (parents.right.page.address != pivot.page.address) {
            descents.push(parents.right);
          }
        }

        if (parents.left.page.address != pivot.page.address) {
          descents.push(parents.left);
        }

        // Descend to the pages we want to merge. Note that if we're on the
        // penultimate page, the next descent will follow the index we
        // decremented above, the leaf page to the left of the keyed page,
        // instead of going to the right-most leaf page.
        descents.push(pages.left = parents.left.fork());
        pages.left.descend(pages.left.left, pages.left.level(parents.left.depth + 1), check(atLeftPage));
      }

      // We use `left` in both cases, because we don't need an index into the
      // page to merge, only a lock on the leaf page.
      function atLeftPage (callback) {
        descents.push(pages.right = parents.right.fork());
        pages.right.descend(pages.right.left, pages.right.level(parents.right.depth + 1), check(atRightPage));
      }

      // When we reach the right page, we can merge the right page we perform
      // our merge.
      function atRightPage () {
        merger(pages, ghosted, check(merged));
      }

      // We check to see that a merge was actually performed. It may be the case
      // that we're merging leaves and they've grown since we made our balance
      // plan.
      function merged (dirty) {
        if (dirty) {
          renameRightPageToMerge();
        } else {
          release(callback)();
        }
      }

      function renameRightPageToMerge () {
        rename(pages.right.page, "", ".unlink", check(rewriteKeyedBranchPage));
      }

      function rewriteKeyedBranchPage () {
        var index = parents.right.indexes[ancestor.address];

        designation = ancestor.cache[ancestor.addresses[index]];

        uncacheKey(ancestor, ancestor.addresses[index]);
        splice('addresses', ancestor, index, 1);

        if (pivot.page.address != ancestor.address) {
          ok(!index, "expected ancestor to be removed from zero index");
          ok(ancestor.addresses[index], "expected ancestor to have right sibling");
          // todo: this is only a problem for this milestone.
          ok(ancestor.cache[ancestor.addresses[index]], "expected key to be in memory");
          designation = ancestor.cache[ancestor.addresses[index]];
          cacheKey(pivot.page, pivot.page.addresses[pivot.index], designation);
        }
        sliceEmpties();
      }

      function sliceEmpties () {
        empties = singles.slice();
        writeBranch(ancestor, ".pending", check(rewriteEmpties));
      }

      function rewriteEmpties () {
        if (empties.length) {
          rename(empties.shift(), "", ".unlink", check(rewriteEmpties));
        } else {
          beginCommit();
        }
      }

      function beginCommit () {
        // **TODO**: If I succeed, how will I know to test the parents for
        // balance?
        // **TODO**: Uh, can't the medic just note that this page needs to be
        // rebalanced? It can force a propagation of balance and merge checking
        // of the parent.

        empties = singles.slice();
        // Renaming pending to commit will cause the merge to roll forward.
        rename(ancestor, ".pending", ".commit", check(unlinkEmpties));
      }

      // Unlink ancestor branch pages that are now empty as a result of the
      // merge.
      function unlinkEmpties () {
        if (empties.length) {
          unlink(empties.shift(), ".unlink", check(unlinkEmpties));
        } else {
          replaceLeftPageToMerge();
        }
      }

      // Move the merged left leaf page into place.
      function replaceLeftPageToMerge () {
        replace(pages.left.page, ".replace", check(unlinkRightPageToMerge));
      }

      function unlinkRightPageToMerge () {
        unlink(pages.right.page, ".unlink", check(endCommit));
      }

      function endCommit () {
        replace(ancestor, ".commit", check(release(propagate)));
      }

      function release (next) {
        return function () {
          // Release locks.
          descents.forEach(function (descent) { unlock(descent.page) });
          locked.forEach(unlock);
          next();
        }
      }

      // Release our locks and propagate the merge to parent branch pages.
      function propagate () {
        // We released our lock on the ancestor, but even if it is freed by a
        // cache purge, the properties we test here are still valid.
        if (ancestor.address == 0) {
          if (ancestor.addresses.length == 1 && !(ancestor.addresses[0] % 2)) {
            fillRoot(callback);
          } else {
            callback(null);
          }
        } else {
          chooseBranchesToMerge(designation, ancestor.address, callback);
        }
      }
    }

    // Merge the leaf page identified by the key into its left leaf page
    // sibling.
    //
    // The key parameter designates the leaf page to merge into its left
    // sibling. The unbalanced parameter is the set of leaf pages that were
    // potentially unbalanced when the balance plan that invoked this merge was
    // constructed.

    //
    function mergeLeaves (key, leftKey, unbalanced, ghostly, callback) {
      // The generalized merge function needs to stop at the parent of the leaf
      // page we with to merge into its left leaf page sibling. We tell it to
      // stop it reaches a branch page that has leaf pages as children. We call
      // these penultimate pages.
      function stopper (descent) { return descent.penultimate }

      // By the time we lock the leaf pages exclusively, their sizes may have
      // changed so that they are no longer candidates for merge. If that is
      // case, we place the pages that were unbalanced before the merge into the
      // set of unbalanced pages we inspect the next time we balance. The leaf
      // page for the key cannot merge with it's left sibling, but perhaps it
      // can merge with its right sibling.
      function merger (leaves, pivot, callback) {
        var check = validator(callback);

        // todo: really, really want page.key
        ok(leftKey == null ||
           comparator(leftKey, leaves.left.page.cache[leaves.left.page.positions[0]].key)  == 0,
           "left key is not as expected")

        var left = (leaves.left.page.positions.length - leaves.left.page.ghosts);
        var right = (leaves.right.page.positions.length - leaves.right.page.ghosts);

        // See if we can fit any more into the left page at the next balance.
        balancer.unbalanced(leaves.left.page, true);

        if (left + right > options.leafSize) {
          // We cannot merge, so we queue one or both of pages for a merge test
          // on the next balancer.
          if (unbalanced[leaves.left.page.address]) {
            balancer.unbalanced(leaves.left.page, true)
          }
          if (unbalanced[leaves.right.page.address]) {
            balancer.unbalanced(leaves.right.page, true)
          }
          callback(null, false);
        } else {
          deleteGhost();
        }

        var index;

        // Note how we do not delete a ghost if the two merge pages together
        // form an empty page. We'll still need a key, so keep the ghost key.
        function deleteGhost () {
          if (ghostly && left + right) {
            if (left) {
              exorcise(pivot, leaves.left.page, leaves.left.page, check(merge));
            } else {
              exorcise(pivot, leaves.left.page, leaves.right.page, check(merge));
            }
          } else {
            merge();
          }
        }

        function merge () {
          // The right leaf page of of the merged page is the right leaf page of
          // the right page of the merge.
          leaves.left.page.right = leaves.right.page.right;

          index = leaves.right.page.ghosts;

          if (index < leaves.right.page.positions.length) fetch();
          else rewriteLeftLeaf();
        }

        var position;

        // Append all of the records of the right leaf page, excluding any
        // ghosts.

        // Fetch a page from the right leaf page.
        function fetch () {
          // Fetch the record and read it from cache or file.
          position = leaves.right.page.positions[index];
          stash(leaves.right.page, index, check(copy));
        }

        // Append a record fetched from the right leaf page to the left leaf
        // page.
        function copy (object) {
          // Uncache the record from the right leaf page.
          uncacheRecord(leaves.right.page, position);

          // Add it to our new page. The negative positions and lengths are
          // temporary. We'll get real file positions when we rewrite.
          splice('positions', leaves.left.page, leaves.left.page.positions.length, 0, -(position + 1));
          splice('lengths', leaves.left.page, leaves.left.page.lengths.length, 0, -(position + 1));
          cacheRecord(leaves.left.page, -(position + 1), object.record, object.key);

          if (++index < leaves.right.page.positions.length) fetch();
          else rewriteLeftLeaf();
        }

        // Initiate page rewriting by rewriting the left leaf. The remainder of
        // the page rewriting is performed by the generalized merge function.
        function rewriteLeftLeaf () {
          // Remove the positions the outgoing page to update the JSON size of
          // the b&#x2011;tree.
          splice('positions', leaves.right.page, 0, leaves.right.page.positions.length);
          splice('lengths', leaves.right.page, 0, leaves.right.page.lengths.length);

          // Rewrite the left leaf page. Move the right leaf page aside for the
          // pending unlink.
          rewriteLeaf(leaves.left.page, ".replace", check(resume));
        }

        // Continue with the generalized merge function. `true` indicates that
        // we did indeed merge pages and the pages participating in the merge
        // should be rewritten.
        function resume () {
          callback(null, true);
        }
      }

      // Invoke the generalized merge function with our specializations.
      mergePages(key, leftKey, stopper, merger, ghostly, callback);
    }

    // Determine if the branch page at the given address can be merged with
    // either of its siblings. If possible we procede to merge the chosen
    // branche pages, otherwise procede to the next balance operation.

    //
    function chooseBranchesToMerge (key, address, callback) {
      var check = validator(callback),
          descents = [],
          choice, lesser, greater, center;

      // Descend to the branch page we want to test for a potential merge. When
      // we descend, the `Descent` class will track the path to the branch page
      // that is to the left in its `lesser` property and the branch page to the
      // right in its `greater` property.
      descents.push(center = new Descent());
      center.descend(center.key(key), center.address(address), check(findLeftPage));

      // The branch page we're testing may not have a left sibling. If it does
      // the `lesser` property is a `Descent` class that when followed to the
      // right to the depth of the branch page we're testing, will arrive at the
      // left sibling branch page.
      function findLeftPage () {
        if (lesser = center.lesser) {
          descents.push(lesser);
          lesser.descend(lesser.right, lesser.level(center.depth), check(findRightPage));
        } else {
          findRightPage();
        }
      }

      // The branch page we're testing may not have a right sibling. If it does
      // the `greater` property is a `Descent` class that when followed to the
      // left to the depth of the branch page we're testing, will arrive at the
      // right sibling branch page.
      function findRightPage () {
        if (greater = center.greater) {
          descents.push(greater);
          greater.descend(greater.left, greater.level(center.depth), check(choose));
        } else {
          choose();
        }
      }

      // See if the branch page we're testing can merge with either its left
      // branch page sibling or its right branch page sibling. We always merge a
      // page into its left sibling, so if we're able to merge, we obtain the
      // key of the right page of the two pages we want to merge to pass it onto
      // the `mergeBranches` function along with the address of the right page
      // of the two pages we want to merge.
      //
      // In the case of a branch page the key is the key of the leaf page found
      // by descending along the left edge of the sub-tree beneath the branch
      // page. This is used to determine where to pivot in `mergeBranches` to
      // find the left branch page of the merge.
      function choose () {
        var choice, designator;

        if (lesser && lesser.page.addresses.length + center.page.addresses.length <= options.branchSize) {
          choice = center;
        } else if (greater && greater.page.addresses.length + center.page.addresses.length <= options.branchSize) {
          choice = greater;
        }

        if (choice) {
          descents.push(designator = choice.fork());
          designator.descend(designator.left, designator.leaf, check(designate));
        } else {
          release();
          callback(null);
        }

        function designate () {
          stash(designator.page, 0, check(propagate));
        }

        function propagate (entry) {
          release();
          mergeBranches(entry.key, choice.page.address, callback);
        }
      }

      function release () {
        descents.forEach(function (descent) { unlock(descent.page) });
      }
    }

    // Merge the branch page identified by the address found along the path
    // defined by the given key into its left branch page sibling.

    //
    function mergeBranches (key, address, callback) {
      // The generalized merge branch needs to stop at the parent of the branch
      // page we wish to merge into its left branch page sibling.
      function stopper (descent) {
        return descent.child(address);
      }

      function merger (pages, pivot, callback) {
        // Merging branch pages by slicing out all the addresses in the right
        // page and adding them to the left page. Uncache the keys we've
        // removed.
        var cut = splice('addresses', pages.right.page, 0, pages.right.page.addresses.length);
        cut.forEach(function (address) { uncacheKey(pages.right.page, address) });
        splice('addresses', pages.left.page, pages.left.page.addresses.length, 0, cut);

        // Write out the left branch page. The generalized merge function will
        // handle the rest of the page rewriting.
        writeBranch(pages.left.page, ".replace", validate(callback, resume));

        // We invoke the callback with a `true` value indicating that we did
        // indeed merge some pages, so rewriting and propagation should be
        // performed.
        function resume () {
          callback(null, true);
        }
      }

      // Invoke the generalized merge function with our specializations.
      mergePages(key, null, stopper, merger, false, callback);
    }

    // When the root branch page has only a single child, and that child is a
    // branch page, we copy the children of the root are replaced by the
    // children of root branch page's single branch page child. The fill root
    // operation is the operation that decreases the height of the
    // b&#x2011;tree.

    //
    function fillRoot (callback) {
      var check = validator(callback), descents = [], root, child;

      // Start by locking the root exclusively.
      descents.push(root = new Descent());
      root.exclude();
      root.descend(root.left, root.level(0), check(getChild));

      // Lock the child branch page of the root branch page exclusively.
      function getChild () {
        descents.push(child = root.fork());
        child.descend(child.left, child.level(1), check(fill));
      }

      // Copy the contents of the child branch page of the root branch page into
      // the root branch page, then rewrite the root branch page.
      function fill () {
        var cut;
        cut = splice('addresses', root.page, 0, root.page.addresses.length);
        cut.forEach(function (address) { uncacheKey(root.page, address) });
        cut = splice('addresses', child.page, 0, child.page.addresses.length);
        cut.forEach(function (address) { uncacheKey(child.page, address) });
        splice('addresses', root.page, root.page.addresses.length, 0, cut);

        writeBranch(root.page, ".pending", check(rewriteChild));
      }

      // Rewrite the child branch page as an unlink operation.
      function rewriteChild () {
        rename(child.page, "", ".unlink", check(beginCommit));
      }

      // Begin the commit by renaming the root file with a `.commit` suffix.
      function beginCommit () {
        rename(root.page, ".pending", ".commit", check(unlinkChild));
      }

      // Unlink the child.
      function unlinkChild () {
        unlink(child.page, ".unlink", check(endCommit));
      }

      // End the commit by moving the new root into place.
      function endCommit () {
        descents.forEach(function (descent) { unlock(descent.page) });
        replace(root.page, ".commit", callback);
      }
    }

    return classify.call(this, balance, unbalanced);
  }

  // The `key` is the splat array passed to `Strata.iterator` or
  // `Strata.mutator`. If it is zero length, that means no argument was passed,
  // indicating that we should place the cursor at first element in the entire
  // tree. Otherwise, it is the key of the record or insert location to find.
  //
  // We use the length of the splat, instead of a existence check, so that the
  // application developer can use `null` as a key, even though no one should
  // ever use `null` as a key directly. Use a pseudo-duplicate `null` instead.

  // &mdash;
  function cursor (key, exclusive, callback) {
    var sought, descent, check = validator(callback);

    // As good a time as any to reset our user exception tracking.
    thrownByUser = null;

    // Descend to the penultimate branch page.
    descent = new Descent();

    // In theory, we can support null keys, since we can test to see if we've
    // been provided a key value by the arity of invocation.
    sought = key.length ? descent.key(key[0]) : descent.left;

    descent.descend(sought, descent.penultimate, check(penultimate));

    function penultimate() {
      if (exclusive) descent.exclude();
      descent.descend(sought, descent.leaf, check(leaf));
    }

    function leaf (page, index) {
      toUserLand(callback, null, new Cursor(exclusive, key, page, index));
    }
  }

  function iterator () {
    var splat = __slice.call(arguments, 0);
    cursor(splat, false, splat.pop());
  }

  function mutator () {
    var splat = __slice.call(arguments, 0);
    cursor(splat, true, splat.pop());
  }

  function balance (callback) {
    balancer.balance(validate(callback, end));

    function end () {
      toUserLand(callback)
    }
  }

  // Create an in memory mirror of a small b&#x2011tree for display. This is
  // only intended for use against small trees for the sake of illustration.
  function vivify (callback) {
    var check = validator(callback), root;

    lock(0, false, check(begin));

    function record (address) {
      return { address: address };
    }

    function begin (page) {
      expand(page, root = page.addresses.map(record), 0, check(function () {
        unlock(page);
        toUserLand(callback, null, root);
      }));
    }

    function expand (parent, pages, index, callback) {
      if (index < pages.length) {
        var address = pages[index].address;
        lock(address, false, check(address % 2 ? leaf : branch));
      } else {
        toUserLand(callback, null, pages);
      }

      function branch (page) {
        pages[index].children = page.addresses.map(record);
        if (index) designate(parent, index, check(designated));
        else keyed();

        function designated (key) {
          pages[index].key = key;
          keyed();
        }

        function keyed () {
          expand(page, pages[index].children, 0, check(expanded));
        }

        function expanded () {
          unlock(page);
          expand(parent, pages, index + 1, callback);
        }
      }

      function leaf (page) {
        pages[index].children = [];
        pages[index].ghosts = page.ghosts;

        get(0);

        function get (recordIndex) {
          if (recordIndex < page.positions.length) {
            stash(page, recordIndex, check(push));
          } else {
            unlock(page);
            expand(parent, pages, index + 1, callback);
          }

          function push (entry) {
            pages[index].children.push(entry.record);
            get(recordIndex + 1);
          }
        }
      }
    }
  }

  // Attempt to purge the cache until the JSON size of the cache is less than or
  // equal to the given size.
  function purge (downTo) {
    // Iterate until we've reached the desired JSON size or else we've visited
    // every entry in the cache.
    downTo = Math.max(downTo, 0);
    var page, iterator = mru;
    while (size > downTo && iterator.previous !== mru) {
      page = iterator.previous;
      // Pages that are locked cannot be purged.
      if (page.locks.length == 1 && page.locks[0].length == 0) {
        // Deduct the size of the page from the size of the b&#x2011;tree.
        size -= page.size;
        // Purge entry from cache.
        delete cache[page.address];
        _unlink(page);
      } else {
        iterator = page;
      }
    }
  }

  return classify.call(this, create, open,
                              iterator, mutator,
                              balance, purge, vivify,
                              close,
                              _size, _nextAddress);
}

module.exports = Strata;

// ## Glossary
//
// * <a name="map">**map**</a> &mdash; A JavaScript Object that is used as a key
// value store. We use the term *map*, because the term *object* is ambiguous.
//
/* vim: set ts=2 sw=2: */
