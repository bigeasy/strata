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
