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
