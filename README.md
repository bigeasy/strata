<a href="http://www.flickr.com/photos/rickz/2207171252/" title="&quot;The Wave&quot; by rickz, on Flickr"><img src="http://farm3.staticflickr.com/2363/2207171252_6ebe988904_z.jpg?zz=1" width="850" height="567" alt="&quot;The Wave&quot;"></a>

# Strata [![Build Status](https://secure.travis-ci.org/bigeasy/strata.png?branch=master)](http://travis-ci.org/bigeasy/strata)

An Evented I/O B-tree for Node.js.

The docs below are a work in progress; for now, [read the
Docco](http://bigeasy.github.io/strata/).

Read the [Docco](http://bigeasy.github.io/strata/)! Read the
[Docco](http://bigeasy.github.io/strata/)! Read the
[Docco](http://bigeasy.github.io/strata/)!

## Purpose

Strata is part of a collection of database primitives that you can use to design
your own distributed databases for your Node.js applications.

Strata is a **concurrent**, **b#x2011;tree** **primitive**, in
**pure-JavaScript** for Node.js.

A **b#x2011;tree** is a data structure used by databases to store records
organized in large pages on disk.

By **concurrent** I mean that multiple queries can make progress on a descent of
the b#x2011;tree. Multple reads can all navigate the b#x2011;tree
simultaneously, of course. Multple reads can also make progress in the presence
of a write, so long as they are not reading a page that is being written. This
is the equivalence to "threading" in other database engines, but evented for
Node.js.

Strata is a database **primitive**, it is not supposed to be used a as a general
purpose database by it's lonesome, but an interface to a b#x2011;tree and it's
concepts that you can use to create different types database strategies.

## Brace Yourself

The interface to Strata is *not* an API, it is a programmer's interface to
b#x2011;tree concepts. It is easy to use, if you know how a b#x2011;tree works,
but please don't complain about encapsulation; it is not a database engine, it
is a b#x2011;tree structure and the *details are supposed to be exposed*.

The Strata b#x2011;tree interface describes a b#x2011;tree as a collection of
actors, not a collection of objects. A b#x2011;tree isn't all about "pages."
It's about descending, navigating, appending, and balancing a tree. When you
read the code, you're going to find these people-named classes who do things.

Finally, Strata is an ancient project of mine, that began before anyone really
know how a Node.js library is supposed to look. I used closure based objects,
which is a way to go, but most noders use prototype based objects. That's what
I'd do I was to do it all again.

I'm going to cut this whinging in the final `README.md`. It's here to vent my
defensiveness and remind of who my audience is.

## A Note on Examples

All of the examples below assume the following function.

```javascript
function validator (callback) {
  return function (forward) {
    return function (error) {
      if (error) {
        callback(error);
      } else {
        try {
          forward.apply(null, __slice.call(arguments, 1));
        } catch (error) {
          callback(error);
        }
      }
    }
  }
}
```

TK: More about how that works. It's all over Strata.

## Creating a B-Tree

You must create the b#x2011tree  object first, specifying the size of the inner branch
pages as a count of child pages, and the size of the leaf pages as a count of
stored records.

```javascript
function openOrCreate (directory, callback) {
  var check = validator(callback);

  var strata = new Strata(directory, { leafSize: 1024, branchSize: 1024 });

  fs.stat(directory, function (error, result) {
    if (error.code == 'ENOENT') strata.create(check(done));
    else strata.open(check(done));
  })

  function done () {
    callback(null, strata);
  }
}

openOrCreate('/home/alan/strata', function (error, strata) {
  if (error) throw error;
  
  // Do something with an open b#x2011;tree...
});
```

Properties to the constructor...

## Primitive Operations

With Strata you either create read-only iterator, or a read/write mutator. The
mutator is a superset of the iterator so let's start there.
