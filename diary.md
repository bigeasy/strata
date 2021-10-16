## Sat Oct 16 03:23:25 CDT 2021

Have it in my head that all keys should be composite, the should be arrays,
since all the downstream applications use them only as arrays, since the first
downstream application is Amalgamate and Memento and Locket are based on
Amalgmate while IndexedDB is based on Memento.

Then things like partitioning can be based on a slice of the key, not on a set
of functions that do one thing or another, but I'd still have to think it all
through to get it to work. Well, I could transition slowly allowing something
like partitioning continue to be opaque functions, not changing the code, but
starting to change other things into parameters.

It would be significant change and I wouldn't feel it until I worked through the
dependencies. I'd want to walk things back out if it didn't make things
significantly easier to understand and document.

But, it feels like the sort of change I'm going to think about for a couple
months before I finally break down and do it and then forget it was every any
other way. It may seem like a lot of work now, but there is already so much
cruft, like the `branch` versus `leaf` comparators, and I don't know of any
other way to make those work yet, no other application has arrisen outside of
composite keys, while new uses of the compside keys keep presenting themselves.

If I were more intelligent I'd reason about it, and when I've made the change
the reasons will be obvious. What's the word for this? Epistimic? I don't know
until I see it in the code.

Anyway, insisting on compound keys means rewriting all the tests. It is a lot of
work and I can't see all the implications for the downstream applications form
here.

## Sat Oct 16 03:12:02 CDT 2021

Starting to wish I'd avoided the parallel writes since I believe the
Fracture/Turnstile logic to be unnecessarily expensive. In the downstream
applications, when we are writing out the user writes we are writing to the
write-ahead log, which is always just a single append. When we are writing to
the filesystem it is in the background. Probably faster to do the writes in a
simpler single queue somehow.

## Sat Nov 21 17:15:15 CST 2020

Before this is over, I'm going to want some way to share turnstiles.

## Sat Nov 21 15:46:29 CST 2020

Some thoughts on counted and merkelized trees.

Learning from r-tree that pages can be treated as logs. For the r-tree every
insert into a leaf requires updating the bounding boxes of branches, so I'm
getting into the swing updating across leaf and branch files.

Which suggests that for the r-tree we may as well have counted branches if every
insert is likely to write to every page in the page. It's not entirely likely.
If you add Seattle, WA and Miami, FL, there are great many US cities you could
add that would fit within the bounds of the box that contains both of those
cities, but if you're adding cities strictly East to West, you are likely
growing the boundry box frequently.

What occurs to me is that the root would get written to in every write, and it
further occurs to me that you could overcome this problem by maintaining a
maximum depth of counted pages. That is only the bottom two branches of the tree
are counted, or only half the depth of the tree is counted, or some such.

What you get for this is faster inserts and localized counts. You would get
faster count reads because instead of reading the page into memory, you would
read just the count log into memory. We would have to have a separate count
cache aside from the page cache.

And then we could have count logs. Separate logs for each page so that we could
add this feature without having to alter this current implementation of the
b-tree.

As far as merkelization goes, I'm imagining that we can have an append order for
each page. After loading a page we sort by the append order. We calculate the
hash for the page. Then we sort by the record keys again. Seems that we could
also limit this by depth. It gets fuzzy when I think about maintaining an
external log, though. Suppose the next step in loading a page is to load the
merkel log (I'm making up terms here, sorry) and check that hash matches.

I'd considered this at some point and realized that migrating these trees would
not be done by inserting items according to their insert order explicitly, not
by adding them...

Ugh, no. It's always got to be a hash of the entire page, doesn't it? Because
removes are also appended, so we'd have to delay vacuum...

Or does that matter? Hash what's in the log. Seems like if we're mirroring a
tree, so long as the tree doesn't change...

Oh, right they change. How do we mirror a live tree? Starts to seem like
something we do in Memento with a Snapshot iterator.

Also, how do we do versioned counts? Ouch.

Oh, yeah. Our primary tree in Amalgamate can be counted and possibly save us
some trouble, while the merge tables would have to be traversed. Wait, you would
have removes and you'd have to check to see if they actually exist in the
primary tree to determine if they actually decrement the count, so you may end
up loading a bunch of pages anyway.

 * [Choosing a hash function for best performance](https://stackoverflow.com/questions/10070293/choosing-a-hash-function-for-best-performance).

Enough of this for now.

## Sun Sep 20 20:45:19 CDT 2020

Occurs to me that an easy way to do counts or hashes is to just have an
additional file in page directory for counts or hashes. It would be a short file
that would be easy to read, be per leaf, and you could probably use timestamps
on the file to determine if you ought to load the page and recalcuate.

Which would be a place to start. Currently, we're content that a failure to
write a leaf file means that the last few appends are lost, and that we'll
notice and start complaining to user, instead of failing silently and misleading
the user into believing that their data has been saved when it has not.

If we have two files, once that is small containing meta-data, how do we know
that the smaller file is valid relative to the larger file? If the file is a
summary of a branch, then it is a summary of all the children, so how do we know
that this summary is correct?

With Amalgamate we are close to having a write-ahead log, oh, and it occurs to
me now that there is no good way to merge a count or a Merkel tree, not unless
we adapt Amalgamate to only consider itself committed once the stage is merged
into the primary tree, and then to only reference the primary tree in its
iterators. What then is the use, really, of a pre-calculated count? It is only
useful if we are counting by an index. Yes, I suppose that is useful.

Two nascent thoughts, then. Some sort of count cache that is hashed on versions,
some sort of version number for a version set to order that cache, so a version
number that is ever increasing, and then a version set version that is ever
increasing, and now it does seem to make more sense to keep this meta-data in an
external index, not inside the tree. This verison set version number, may as
well implement it and see what it enables.

Second nascent thought is just that if the primary tree is large, and the stages
are small, you could query a count by the primary tree first, and calculate only
those primary tree pages that do not fit somehow.

Finally, if we want to have this merge thing, and we want to have a tree
properties like merkel and count, then we need a definitive tree and need to
expose the three structure through our clever iterators. If the first key in a
primary key page and the last key in a primary page resolve to the same index in
a stage, then there is nothing to search for, and assuming that the stages are
small, oh, but then we're storing first and last keys in our little lookup file,
but, this might be a useful optimization. It is going to want to be pluggable,
let me construct a little cupcake for a page when you've added stuff to it.

Of course, each branch page does have a key range, we could use the branch pages
of the primary to determine these ranges.

## Sat Aug 22 21:18:25 CDT 2020

git log -n 1 bc45430aedcb1dc35256d321b83009ce28821f2f

## Thu Aug 20 08:51:02 CDT 2020

At some point we went from records to key/value. Used to be the case that there
was an extractor that would extract the key from the record. This makes sense
when you have an object store that uses a native key, you're not going to
duplicate the key information in the serialized format.

With this, however, you do not know how to serialize the key in a branch which
wants to store only the key, not the full record. You'd still have to specify a
key serializer. We wouldn't reduce the number of functions you'd have to provide
a `Strata` at construction, in fact we'd increase them by adding an `extractor`.

We could make this both key/value and extracted key with the new parts record
format. We can make insert take an array of parts, the first part is the key.
The key does run though an extractor. There is also a separate key serializer.
There's a bunch of stuff. Most of this stuff is going to be there anyway, only
the extractor is an addition.

Which makes `Strata` itself a little less usable, a little more useful, and all
to save a little bit of space on disk. Shouldn't effect performance. Do not need
this to implement `levelup`, but would be useful for IndexedDB, I think.
Certainly for Memento.

## Sat Feb 22 09:07:42 CST 2020

When we last looked as this, apparently we where working through the split, so
we need to continue with it by first draining the root, then splitting a branch.

Also, I believe I left off by porting to Python and Crystal, so rabbit holes.

Seems like the next thing to do is to create a Strata with a split size of 4 and
attempt to get it to drain the root, which means writing drain the root. While
I'm at it, I'm going to attempt to document all these asynchronous queues.
