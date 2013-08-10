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

## Changes for Next Release

 * Upgrade Proof to 0.0.31. #113.
 * Return an comparative integer from `Cursor.insert`. #112.
 * Peek of next page during insert with read lock, release immediately. #111.
 * Mutator should return false if index is to low instead of asserting. #110.
 * All Strata constructor options should be in the `options` hash. #109.
 * Fix aspect ratio of `README.md` image. #108.
