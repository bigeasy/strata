An Evented I/O B-tree for Node.js.

Strata is part of a collection of database primitives that you can use to design
your own distributed databases for your Node.js applications. I call this
collection the Strata Universe. It culimates in Locket, a pure-JavaScript
implementation of LevelDB.

Strata is a **concurrent**, **b&#x2011;tree** **primitive**, in
**pure-JavaScript** for Node.js.

A **b&#x2011;tree** is a data structure used by databases to store records
organized in large pages on disk.

By **concurrent** I mean that multiple queries can make progress on a descent of
the b&#x2011;tree. Multiple reads can all navigate the b&#x2011;tree
simultaneously. Multiple reads can also make progress in the presence of a
write, so long as they are not reading a page that is being written. This is the
equivalence to "threading" in other database engines, but evented for Node.js.

Strata is a database **primitive**, it is not supposed to be used a as a general
purpose database by it's lonesome. Strata an implementation of a b&#x2011;tree
and it's interface exposes b&#x2011;concepts. If you're using Strata you're
either implementing a database engine, or your taking your indexes and queries
into your own hands.

## Installing

Install from NPM.

```console
npm install b-tree
```

## B-Tree Properties

*TK: Unique keys, but duplicate keys are super easy to fake.*

## Creating a B-Tree

You must create the b&#x2011;tree object first specifying a directory where
records are stored.

```javascript
function createOpenClose (directory, callback) {
    var strata

    create()

    function create () {
        strata = new Strata(directory)
        strata.create(created)
    }

    function created (error) {
        if (error) return callback(error)

        strata.close(reopen)
    }

    function reopen (error) {
        if (error) return callback(error)

        strata = new Strata(directory)
        strata.open(opened)
    }

    function open (error) {
        if (error) return callback(error)

        strata.close(callback)
    }
}

createOpenClose(function (error) { if (error) throw error })
```

### Extractor and Comparator

Strata will store any JSON object or value. By default it compares using the
JavaScript comparison operators `<` and `>`. If you're string a `String` or a
`Number` the default comparisions will work just fine.

If you're storing objects you're going to want to order by one or more of the
object's properties. To do this you specify an `extractor` property to extract
the key properties from the object and a `comparator` to compare the extracted
key.

```
function extractor (person) {
    return {
        firstName: person.firstName,
        lastName: person.lastName
    }
}

function comparator (left, right) {
    var compare = left.lastName < right.lastName
                ? -1 : left.lastName > right.lastName
                     ? 1 : 0
    if (compare != 0) {
        return compare
    }
    return left.firstName < right.firstName
         ? -1 : left.firstName > right.firstName
              ? 1 : 0
}

var strata = new Strata({ extractor: extractor, comparator: comparator })
```

Note that neither the `extractor` nor the `comparator` are stored in the Strata
tree. You must provide them when you construct a `Strata` tree object both when
you are creating a new Strata tree or opening an exiting one. That is, you need
to know the type of data stored in the tree.

Because Strata is not a general purpose data store, it is a b&#x2011;tree
privitive.

### Searching the B&#x2011;Tree

With Strata you either create read-only iterator, or a read/write mutator. The
mutator is a superset of the iterator so let's start there.

```javascript
// Returns true if the key exists in the tree.
function hasKey (strata, sought, callback) {
    var found

    strata.iterator(sought, cursor)

    function cursor (error, cursor) {
        if (error) callback(error)
        else {
            found = cursor.index >= 0
            cursor.unlock(unlocked)
        }
    }

    function unlocked (error) {
        if (error) callback(error)
        else callback(null, found)
    }
}

hasKey(strata, 'c', function (error, exists) {
    if (error) throw error
    if (exists) console.log('I found it.')
})
```

In the above, we create a read-only `Cursor` using the `Strata.iterator`
function. That returns an iterator that holds a shared lock on the leaf page
that either contains the records for the given key, or else would contain the
record for the given key if it existed in the leaf page. The `Cursor` says that
the record is here if it is in the tree, or it should go here if it's not.

The default `Strata` b&#x2011;tree will have defaults for branch size and leaf
size, but you can adjust those as well.

Properties to the constructor...

#### `new Strata(location[, options])`.

Constructs a new b-tree that stores its files in the directory provided by
`location`. It does not open or close the b&#x2011;tree.

#### `options`

`new Strata()` takes an optional options object as its second argument; the
following properties are accepted:

 * `extractor`: A function that extracts the key from the record.
 * `comparator`: A function that is used to compare keys.
 * `leafSize`: The maximum size in records of a leaf page before it is it split.
 * `branchSize`: The maximum size in child pages of a branch page before it is
   split.
 * `checksum`: A cryptographic algorithm to use as a hash, or a checksum
   function to validate each line in a leaf page, and the contents of a branch
   page.

#### `strata.open(callback)`

Opens the b-tree.

#### `strata.create(callback)`

Creates a new, empty b-tree. It will raise an exception if there is *anything*
in the location directory.

## Searching and Editing

You search and edit the b&#x2011; separate from editing it.


If the `Cursor.index` property is zero or more, it is the index of the record in
the leaf page. If the `Cursor.index` property is less than zero, then it's
compliment is the index of where the record should go in the leaf page.

In the `hasKey` function above we simply return whether or not the record exists
based on the cursor index.

### Scanning the B-Tree

*Ed: The following is stupid wrong and stupid.*

```javascript
var range = cadence(function (async, strata, start, stop) {
  strata.iterator(start, check(atLeaf));

  function atLeaf (cursor) {
    fetch(cursor.index &lt; 0 ? ~cursor.index : cursor.index);

    function fetch (index) {
      if (index &lt; cursor.length) {
        cursor.get(index, check(push));
      } else {
        cursor.next(check(advanced));
      }
    }

    function push (record) {
      if (record &lt; stop) {
        found.push(record);
        fetch(index + 1);
      } else {
        done();
      }
    }

    function advanced (success) {
      if (success) done();
      else fetch(0);
    }

    function done () {
      cursor.unlock();
      callback(null, found);
    }
  }
}

range(strata, 'c', 'i', function (error, found) {
  if (error) throw error;
  console.log(found);
});
```

### A Note on Examples

I use a control-flow library of my own creation called
[Cadence](https://github.com/bigeasy/cadence) that I'm just crazy about.

The primary benefit of Cadence it asynchronous try/catch error handling. This
has made it very easy to create deeply nested asynchronous operations, yet have
errors propagate up to the caller, the context that comes from maintaining a
stack and unwinding it on error.

Secondary benefit is that Cadence always uses a trampoline. This allows me to
cache aggressively without having to worry about blowing the stack.

Finally, Cadence is pure-JavaScript and old fashioned JavaScript. It doesn't
depend on language features that have yet to drop. No transpilers.
