# Strata Design Diary

 * It is worth nothing that we split a page into however many pages, we don't
   recursively split, we could have, it would have been much easier, but we
   didn't, so here we are. However, we don't merge however many pages at once,
   mercifully, and I don't believe we're going to merge however many pages at
   once any time soon.
 * Need to be able to say that you can do big table with strata, so that means
   that you set `leafSize` to a finite size and `branchSize` to size that is
   never going to be reached; `Number.MAX_VALUE`.
 * Should `merger` accept a `left` and `right` argument?
 * Why is `designate` not defined in terms of itself?
 * Should `uncacheKey` or `uncacheRecord` be called automatically by `splice`?
 * The `balance` function needs to return a boolean to indicate that it did
 completely balance the tree. Or that there was nothing to balance. Keep
 balancing until you get a zero then timeout for a while.
 * Consider creating a `balanced` function to determine if the tree is optimally
 balanced. We would create such a function because it we might change our merge
 and split algorithms in the future, causing our tests to fail when they balance
 the tree, producing a good balance, but a different balance than the current
 algorithm.
 * Reassure yourself that you'll never delete the ghost of a key on the left
 most leaf page.

Stash as part of cursor.

Is this evolved thinking? Yes, I do need, very much need, I do need a way to
check if things are copacetic, but...

Not yet. That would make this take even longer. It is a stop along the way to a
release to get something done right now. Let's finish all our branching and
merging before we implement `copacetic`.

The `copacetic` method does not have to be too complicated at this point. Also,
it ought to be able share the same thread as the balancer, it can navigate
leaves first, to ensure they are linked correctly, then navigate the entire
tree, ensuring that the link and balance is correct.

### Unlink

Maybe you use `fs.unlink` and `fs.rename`, which would save you some naming
troubles. No, no, no. The term `unlink` is overloaded in the Strata source an I
AM GOING TO LIVE WITH IT.

## Concurrency

Things to test for concurrency.

 * Test that you do not purge a cache page that is being held for balance.
 * Actually, on that note, make sure that you do free up the cache of a page
 that is being held for balance.

## Notes

Generalized playback function, same as used by medic, use it everywhere. Then
you know that once you get your stuff aligned, it will fall into place.

Feel good about reading and writing, also, try to feel good about concurrency,
then you'll be able to ship, but when bad things happens, it will only be in
balance, no data will be lost.

Also, you can test balance using a random generating, putting in a die here and
there, and then watching as something hits it. Then restart with a balance,
you'll probably hit it again.

Copacetic simply descends the tree in order and checks that the right pages link
correctly, oh, and I suppose that the items in the page are in order, and then
you can clear the cache. It cannot run concurrently with balance. It also checks
that all the cached pages add up so you can't clear the cache, but that is
probably a syncrhonous operation.

## Quality

Create a viewer program, but I'm not sure if it is an ncurses program, or an
HTML5 program. I can reach a wider audience with HTML5, but I might get more
development done more quickly with ncurses.

## Merging

Going to have to consider the different forms of merge, chosing the right
candidate, etc. Not sure if I'm up for the battery of tests before Strata 0.0.3,
but then again, once they're built, it ought not be too difficult.

## Names

Maybe rename `deleteGhost` to `shiftGhost`.

## Length Race Condition

Why does the length need to be stored in the page. Can't the balancer grab the
length into it's nodes? Yes. It is opportunistic. Probably much simpiler.

## Shifting Ghosts

What is so important about deleting ghosts? It seems like a silly tidy when
we've yet to implement vacuum. I suppose it makes vacuum easier because vacuum
will always faithfully rewrite the page, ghosts and all, so it can move across
the leaves without descending.

## Binary Leaves

A leaf format is this:

 * A terminatator, currently '\n'.
 * A checksum, currently SHA1, but always a fixed length.
 * Stuff inbetween the checksum and terminator.

We could easily make the terminator `0xdeadbeaf` or similar, and the checksum
could be a binary integer, and the body could be JSON.

There might be an optional length. If it is missing, then we're going to assume
that the terminator is unique, or else we're going to scan and keep scanning
until we find a buffer that satisfies the checksum. We put the length before the
terminator we can read backwards easily as we are doing now.

This is fantasitic. Do it.

## Inbox

 * `nextTick` behavior ought to make coverage happen quickly, especially a
 combination of `nextTick` behavior and purge.
 * Splitting out the read write lock would create an additional primitive for
 database design.
 * Creating a r-tree would be yet another primitive for database design.
 * Naming functions build by validator `check` is confusing.
 * Wondering why I don't put `leaf` and `branch` onto the file names as
   suffixes, and also, not sure I care if the files have the prefix `segment`.
   It is going to be easier to format the file names without that.
 * Since I'm already doing negative and positive inside strata, why don't I use
   even and odd? That would be consistent with the file system, one
   representation everywhere.

## Delayed Writes

For block writes, with a nice interface, wouldn't it be nice to have `unlock`
take a callback, which would be a way to say, write before unlocking.

However, it would it might just as good to gather up dirty pages and flush them
as part of an global exit. You might luck out and flush out the results of two
descents in one page flush. How often would be you be that sort of lucky that it
would warrant an early flush.

## Position Array Jumping

I'm not sure what I had against reading backwards from the end of leaf
page. We're going to read those records if we jump, we're going to scan
for newlines while we do it, and we're going to keep the objects we
read.

No, it doesn't help you to jump back to it, except maybe to look for the
leaf page to the right, but that wouldn't be a part of normal operation,
only recovery. At times you consider ways to make counted B-Trees
simpiler by caching the record count, but why cache the record count and
not the leaf key? In order to get to the record count, you would have
had to have already read in the leaf key, which means reading back to
the positions array.

Not jumping to it, mind you, because if you jump to it, you won't know
if the first entry is the key, until you replay the log.

Oh, wait, no. The positions array does contain the key. The first value
does not change except for merges and ghost deletions, the operations
that write out a position array. We can add the key to that record. Now
we can get the count from the last record and key by jumping to the
positons array.

Plus, we can jump to the position array and load it for the key, but
leave the leaf page in an unloaded state. If we've only cracked it to
get the key, it's primed and ready to read.

But, ultimately, I believe I'm going to implement counted B-Trees using
an additional Strata tree.

## More Caching

If we do position array jumping, we might open up a read stream to begin to read
the leaf page, or an initial buffer, but leave it there, not proceeding until
someone actually visits the leaf page. We can put this information in the
Magazine perhaps in a separate Magazine for partially read pages?

## Breaking the Rule of Append

I'm struggling to fight file girth, but it occours to me that the reason I'm
struggling so much is that I've make sure that the only writes are appends. I
like the property because it favors durability. I know that I'm only ever
growing the file this way.

However, I could, as easily add a record to the end of the file. That record
could be a footer. It could contain all our housekeeping data. I could
overwrite that footer at each new write.

Which is why I believe I need to add a footer record, even though, for now, I'm
going to keep the only-append. With it, overwrite becomes an option. If anyone
notices or cares about the girth, their is an answer, a place to go. Until then,
every leaf page file is going to be a log.

In fact, if I am going this route, why don't I simply write out the key over and
over again?

## Quick Split and Merge; Log then Rewrite

A number of different situations for writing can be more live than they are now.

Currently, we're locking a branch page while we rewrite a leaf page. That is
going to be a bottleneck in performance. Holding onto an exclusive lock of a
leaf page is bad, but holding onto an exclusive lock on a branch page is worse.

Since we're separating locking from caching, one thing we can do to improve
performance is to cache everything prior to performing the split or merge. We
can descend the tree as a reader, visiting each page that will participate in
the split or merge, adding an additional reference count to the cache.

If we're doing time series data, the balancer can suggest that instead of
splitting, we simply add a new page to the end of branch parent, or else split,
but split by truncating, instead of splitting in half, or have a simple
heuristic which is that if the page to split is the right most page, we split by
truncating, which might mean we have a degenerate last page for most trees, but
a tightly packed tree for append trees.

Now that branch pages are logs, we only need to write our changes to the log
while we exclusive lock, but we can downgrade to a shared lock while we rewrite
the branch page.

Logged splits would be much more difficult. We would write to the page our
intention to split the page in a house keeping record of some sort. That would
say, hey, we split this into three pages, but how does that work in memory? Do
we have an abstraction that is a cached pseudo page where new writes are
appended to the current leaf page log, even as we copy records into new page
files? The pseudo page would have to update both the new cleaned up page, and
the old split page. Actually, that seems rather easier, to simply say, here are
two new pages in memory, update them, now update this old version of the left
page. When the left page flushes, this write is committed. The writes to the new
pages do not have to flush.

What do you write out onto disk when you split to make the split occur as
quickly as possible? Maybe a footer record that says "coming soon?" How do you
read the actual page it references, when it is going to be to the left of the
reference? I suppose when you split, the actual page stays linked as the right
most page, it's left siblings are references.

Merges could work the same way, we merely log the intent to merge, but while
we're writing out the new merged page, iterators will land on the current left
page, and iterate to the right page, which is no longer linked into the tree.

Keep in mind that, if we end up blindly appending stuff, we might want to
replace insert and delete indexes with the actual keys. That would only change
delete log entries, since we can extract the key from the record.

## Null Keys

No. We've already decided that there are no duplicate keys, but we've provided a
solution, a simulated duplicate key using a series value. Now, what is going to
be more common, duplicate keys? Or an index that permits a single null?

Shaking my head gear clear of all this hanging on.

## Changes for Next Release

 * Upgrade Proof to 0.0.31. #113.
 * Return an comparative integer from `Cursor.insert`. #112.
 * Peek of next page during insert with read lock, release immediately. #111.
 * Mutator should return false if index is to low instead of asserting. #110.
 * All Strata constructor options should be in the `options` hash. #109.
 * Fix aspect ratio of `README.md` image. #108.
