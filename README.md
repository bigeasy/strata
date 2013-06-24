<a href="http://www.flickr.com/photos/rickz/2207171252/" title="&quot;The Wave&quot; by rickz, on Flickr"><img src="http://farm3.staticflickr.com/2363/2207171252_6ebe988904_z.jpg?zz=1" width="850" height="567" alt="&quot;The Wave&quot;"></a>

# Strata [![Build Status](https://secure.travis-ci.org/bigeasy/strata.png?branch=master)](http://travis-ci.org/bigeasy/strata)

An Evented I/O B-tree for Node.js.

# Purpose

Strata is part of a collection of database primitives that you can use to design
your own distributed databases for your Node.js applications.

Strata is a **concurrent**, **b-tree** **primitive**, in **pure-JavaScript** for
Node.js.

A **b-tree** is a data structure used by databases to store records organized in
large pages on disk.

By **concurrent** I mean that multiple queries can make progress on a descent of
the b-tree. Multple reads can all navigate the b-tree simultaneously, of course.
Multple reads can also make progress in the presence of a write, so long as they
are not reading a page that is being written. This is the equivalence to
"threading" in other database engines, but evented for Node.js.

Strata is a database **primitive**, it is not supposed to be used a as a general
purpose database by it's lonesome. The interface to Strata is *not* an API, it
is a programmer's interface to b-tree concepts. It is easy to use, if you know
how a b-tree works, but please don't complain about encapsulation. It is not a
database engine, it is a b-tree structure and the *details are supposed to be
exposed*.

## Primitive Operations

With Strata you either create read-only iterator, or a 
