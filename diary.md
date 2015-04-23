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
 * Use no-op line to test conditions that you want to see tested in coverage.

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

### Cursor Creation

Want to keep my stuff consistent.

```
strata.iterator('a', function (error, cursor) {})
strata.iterator(strata.left, function (error, cursor) {})
strata.iterator(strata.right, function (error, cursor) {})
strata.iterator(strata.leftOf(key), function (error, cursor) {})
```

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
probably a synchronous operation.

## Quality

Create a viewer program, but I'm not sure if it is an ncurses program, or an
HTML5 program. I can reach a wider audience with HTML5, but I might get more
development done more quickly with ncurses.

## Merging

Going to have to consider the different forms of merge, choosing the right
candidate, etc. Not sure if I'm up for the battery of tests before Strata 0.0.3,
but then again, once they're built, it ought not be too difficult.

## Names

Maybe rename `deleteGhost` to `shiftGhost`.

## Length Race Condition

Why does the length need to be stored in the page. Can't the balancer grab the
length into it's nodes? Yes. It is opportunistic. Probably much simpler.

## Shifting Ghosts

What is so important about deleting ghosts? It seems like a silly tidy when
we've yet to implement vacuum. I suppose it makes vacuum easier because vacuum
will always faithfully rewrite the page, ghosts and all, so it can move across
the leaves without descending.

## Binary Leaves

A leaf format is this:

 * A terminator, currently '\n'.
 * A checksum, currently SHA1, but always a fixed length.
 * Stuff in between the checksum and terminator.

We could easily make the terminator `0xdeadbeaf` or similar, and the checksum
could be a binary integer, and the body could be JSON.

There might be an optional length. If it is missing, then we're going to assume
that the terminator is unique, or else we're going to scan and keep scanning
until we find a buffer that satisfies the checksum. We put the length before the
terminator we can read backwards easily as we are doing now.

This is fantastic. Do it.

*Update*: What I've come to is something that is counted, but still plain text.

I'd given a lot of thought to how to pack a binary file format, because that's
what binary makes you think. Somehow, coming back to Strata after a ways away,
I'm becoming better at seeing the trade-offs inherent in programming. I can see
the value of my plain text file format, it has not receded as Strata has
progressed. It is always reassuring, when things are working as poorly, when a
test won't pass and won't to open these files and have a look.

## Log Format

In considering error handling, I'm considering extracting the log format to a
separate project so that I can test I/O and perhaps find a way to focus on I/O
errors. The only errors returned from Strata ought to be I/O errors and full up
programming errors are asserted.

## Errors and Corruption

Corruption detection needs to be more `Error` and less `ok`. I'm asserting
properties of a file. Maybe, I need to check the checksum. If the checksum is
valid, then anything about the data that is invalid is an assertion. We're using
a pretty strong checksum.

The only opportunity to actively corrupt data is a failed append, probably
because the disk is full, but all of our other operations are copies and moves.
We can add a step to our move into place commits, where instead of unlinking the
file to replace, we rename it with a suffix like `.outgoing`. `unlink` and
`rename` are unlikely to fail independently of other file I/O operations, but if
they do, if we fail to move the new file in place, we have the old file.

Thus, the only real worry is the append, and that only occurs during insert or
delete, so we can have a severity. If we have a corrupted write, if write fails,
we raise an exception with the greatest severity. If a balance copy fails before
commit, that is not as severe. In the midst of commit is more severe. The
inability to read records, hmm... Is that an error? What if the checksum fails?

Are descents always unlockable? What about before they obtain their first lock?

How are we handling an obtained lock followed by a failed read? Who unlocks?
Right now it propagates all the way up the stack.

Surprised that I'm not holding locks or double releasing, which is great. At
some point I must have put a lot of thought into this.

It would be better to hold an additional lock when forking a descent so that the
page for a descent always holds a lock. Currently, the descent takes ownership
after it descends an initial level. This allows descent forking to work. The
fork won't hold a lock on a page unless it descends a level. Also, to keep the
descent algorithm simple, and to account for the initially unlocked page, we
descend from a dummy branch page that has the root branch page it's only child
and the index is set to the only child. I don't know which came first, the dummy
page, or leaving the first page unlocked, but skipping unlocking on the first
descent accounts for both cases.

Our shared lock is already reentrant, obviously. Sequester needs to either
provide for a reentrant exclusive lock, but that would mean tracking the
"thread" with an id of some sort. If an exclusive lock is from the same "thread"
it is acquired as a reentrant exclusive lock, otherwise it is queued to run
after the current exclusive lock is released. Shared locks won't care about the
"thread", but will still accept the id so that the interface is consistent.

An interface that will permit incrementing the lock count that works for both
shared and exclusive would be simpler, but we won't be able to bandy about the
word reentrant. The concept probably requires more complexity, which we would
have to add in order to have a problem that reentrancy could solve.

This will work or the program will fail. This needs to be documented in
Sequester, Strata and Locket; that you can't throw an exception from a callback
and expect it to propagate. They don't. We don't want to swallow errors, but
when we unlock, and invoke other descents, and possibly have user code raise
exceptions, it is very difficult to test that the tree's state will be
preserved.

Sensing that I'm up against a ledge with this. I'm not interested in
collaborating with users who don't yet understand the limits of try/catch in
node. For an initial release, it is enough that Strata is able to recover from
any form of I/O error and leave the tree in memory, it's cache and it's locks,
in a consistent state.

### More Errors

Trying to think of how to handle a failed write. What are the recoverable cases?
How do you deal with a full disk, for example?

Offhand, I'd say that you give up on the balance, and any changes like the ones
above, any in-memory changes, you dispose of, so rather than set it back, you
remove the page from the cache on error, then it can be reread, which ought to
be interesting, because their might be a truncated last line or some such.

In that case, we'd want some way of marking the page as broken, dirty, and not
writing to it. Makes me wonder what other databases do when problems arise.

For now, I just need to get an error to come up and out of Strata and someplace
where someone can email it to me, where I can begin to see the possible failure
states. I'm sure that if a disk is full, and you're collecting data, then people
are not going to be able to make progress, their programs can't make progress
anyway.

Also, with Journalist, I'm not able to `close` because I'll write a footer onto
an error, so I need to do something else, like `scram` the file.

Errors need to become events, what are they? Cannot read, cannot write and
cannot replace. Cannot write matters if I'm inserted, deleting or exorcising.

Here's a thought, now that I have a cache of open file handles, why not have a
`scram` method in Journalist, it would take a stage and scram all the open files
on that page. This can be done once on exit of balance operations.

The `scram` method can also accept a callback, have a closer of sorts, or
provide an iterator, or no, hmm...

Thinking iterator, but a callback will do, that can iterate over all of the open
file handles and call the callback with the resolved file name and the extra
data that is provided at close. For Strata, this is the page, so the page can be
marked as, hmm...

Okay, so no, we're only interested in pages that had write failures, so we do
need to catch that error and mark our page at the time of write or close.

The reality of it is that we're going to need to have some control outside of
the tree. The tree writes are atomic. We can't be shutting down the writes
within the tree itself. There needs to be a queue outside the tree. Yes, balance
is a big operation, but we stop when something goes wrong, put the rest into the
next balancer.

Each operation then, needs to also undo itself, if we cannot move a rewrite
forward, for example. If we can't replace or remove files, then the tree is
hopelessly corrupt and the writing needs to stop.

A full disk is the most likely issue, though.

## Records and Keys

I'd imagined to use Strata to create a database that stores objects and the keys
would be defined from within those objects. There would be a means of
serializing these objects that works for both keys and records.

With key/value stores, the key is not within the object, so the interface needs
to amalgamate the key and value.

This is confusing and I've thought about it before. It is a trade off and there
is no obvious way in which the complexity can be reduced. If you extract the
key, you need to find some way to serialize it once again, immediately, to get
the length of the key, in order to have a value for the heft of the page.

## Compression

First, this morning, I'm thinking compression and I'm thinking, no I'd rather
not, because Strata offers control and makes things generally accessible to
developers and compression adds complexity for little gain. If you really want
to compress your payloads, maybe compress them before you write them to the
database?

Or maybe, if you'd like, we can gzip the entire leaf page file, and catenate to
an unzipped file. Then when we vacuum we can run it all again, but you're asking
that the entire page be loaded into memory, which is a trade-off, and you're
going to have to decide, but it is a full trade-off. Using `gzip` against a full
leaf will get the best compression imaginable; slow but efficient.

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
 * Can't I write the distance between additions and subtractions and general
   dirtiness into the footer of the page?

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
simpler by caching the record count, but why cache the record count and
not the leaf key? In order to get to the record count, you would have
had to have already read in the leaf key, which means reading back to
the positions array.

Not jumping to it, mind you, because if you jump to it, you won't know
if the first entry is the key, until you replay the log.

Oh, wait, no. The positions array does contain the key. The first value
does not change except for merges and ghost deletions, the operations
that write out a position array. We can add the key to that record. Now
we can get the count from the last record and key by jumping to the
positions array.

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

I'm struggling to fight file girth, but it occurs to me that the reason I'm
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

*update*: Still bothers me, the girth, but if I'm vacuuming frequently, it won't
be such a bother, will it? If vacuum turns out to be something that is done
regularly, then investing in the rewind will not have been worth it.

But, I'm starting to not like append only, because it only protects against a
certain type of error that could be caught by testing. It starts to feel like
type safety, a talisman against corruption, one more thing that we do that make
it impossible, impossible I say! to corrupt files because we're only ever
appending and the operating system will protect us from ourselves.

Yes, vacuuming is something that needs to be done, but in the mean time, all
that cruft. I don't know why I don't err on the side of tidy.

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

Currently, branch page updates are handled by rewriting the pages, locking the
page while they are being rewritten, then using the atomic nature of a rename to
put the new pages in place. The lock, however, blocks other descents from making
progress, so we want to hold it for as short a time as possible, not for the
duration of an entire rewrite.

This is making me consider some sort of a balance plan file, or maybe just a
balance intention file that has the instructions for a single balance written to
the directory. During recovery, this single balance is performed. Alternatively,
there could be a directory that has a file for each plan in the balance, and as
the balance is performed, a file is removed, the files are ordered by a numeric
name.

This is possible because we've made the balance thread of execution separate
and it's goal is not to be lightening fast, but to be lightening fast about
holding exclusive locks. If the update to the branch pages is done with an
append to a file, then that is faster than copying the entire file, or at least
one imagines it would be.

The merges could simply be a log file, actually. Writing out JSON per line for
each action and then logging the completion of each action, in addition to
logging any follow on actions. Thus, you have a plan to split a page, so you get
to the page and create a new page that is an empty split, just a pointer to the
page it split from, update it's parent branch page by adding a key to the page,
which let's say is the root, so there is nothing more to do (the root does not
need to split.) When you create the empty split you add an entry in the merge
log that says to come back and complete the split, you add an entry to the merge
log to come back and rewrite branch page the way it is rewritten now.

Until it is rewritten inserts get appended to the old page file, but inserted
into the correct in memory page object. A rewrite needs to hold a read only lock
on all the split pages, to copy them, but it can do it in two steps, once for
each page, then again holding all pages to check to see if anything changed,
maybe even having some queue of operations to check, instead of having to go
back and try to discern the differences, simply an array, that if present
gathers up writes.

Except that this now means that we want to hold the pages in memory so that they
are not flushed and those writes lost, and we want to do that independently of
the read and write lock mechanism.

## Most Lively Balance

*update*: Now that I've got keys in branch pages here's what occurs to me:

Currently when we split or merge we lock a branch page exclusively and block out
a sub-tree. In that subtree we rewrite pages, performing a lot of file I/O. This
is slow and it blocks progress of read/write descents. It is not necessary. The
only thing that needs to be committed to file is the change in the balance of the
tree.

Here's a mode of operation. Using shared locks, create an annotation it the leaf
page, then create new branch pages by writing out the keys, but put them to one
side as replacements. Now we're not really using our branch pages as journals.
They are always whole files. We move them into place using `unlink` and
`rename`.

The rewriting is done with a shared lock, but it is not reflected in the pages
in memory. Each branch page can hold a dummy branch page that has been
rewritten. We then descend the tree locking the pivot exclusively, then moving
it's dummy into place in memory and moving its rewrite into place on disk.

Now we no longer care about the race conditions of the balance plan. In fact,
the balance plan doesn't make much sense since we do things one step at a time
anyway. We'll need to look at whether or not we want balance planning to be
internal to Strata. We do want to have some automatic behavior though.

Thus, currently, if a page looks as though it needs to split, then it is put
into a plan for splitting. When we reach the page, we check again to see if it
still needs split, then split and propagate the split. I say, when we decide if
something needs to be split, this race condition is futile. It could be the case
that in the time it took us to determine that the page needs to be split that it
does not need to be split, but it could also be the case that the page no longer
needs split the moment after we've split it.

However, in the case of split, we can't split an empty page, or a page with only
one element, so we may have to surrender that split.

Thus, we choose to split a page, so off we go to split it. We read lock on our
way down. We choose partitions. We then update a copy of our pages. We write out
replacements. We write out a stub of the split page.

Then we descend again, this time for locking. We assert that the partition is
still valid, oh, but it is always valid, for even if we've since emptied that
page, we can always keep the partition, so no timeout. With everything locked
exclusive, or on the way down locking exclusive, we put our copies in place,
then we get to the root and copy the current page to a dirty file, rename it,
and then put out stubs, plural, in place. Stubs contain position arrays, but the
positions are in the dirty file.

That was exclusive for only a moment, so we get to continue. We now write out
replacements for the stubs, which is going to be slow and shovel a lot of bytes,
but that's the way it goes, but it's also a shared operation.

In fact, it doesn't need to lock the page at all. It can just copy the old
legacy file to new files, reading through the positions array at was any copying
the records over to their new files. It can then get a read lock in order to
replay the stub but play it into the new files. Then an exclusive lock to move
the pages over.

And then if it is out of balance, then it is out of balance.

**Horrible problem** is that moving away from the current balance plan means
that we'll end up breaking tests, so we may have to create it in parallel.

Oh, that's anguish, that feeling I just felt.

*Inbox*:

 * Play the log and use the positions array as a map to determine if the record
   has survived deletion.

## Null Keys

No. We've already decided that there are no duplicate keys, but we've provided a
solution, a simulated duplicate key using a series value. Now, what is going to
be more common, duplicate keys? Or an index that permits a single null?

Shaking my head gear clear of all this hanging on.

## Error Propagation

I've got a problem now, and it's one that I solved in Cadence. I've got a
wrapper around my callback that simplifies error handling, but if I've called a
callback in user space that throws an exception, I'm going to swallow that user
space exception, when really it was supposed to propagate.

No, not quite solved in Cadence.

Running into a problem with Strata where if I throw an exception from within
Strata, it is likely to get caught by the `validate`. I've already begun work to
solve this problem here. How do you handle errors that are deeply nested, if
you're using try/catch blocks to to convert exceptions into errors?

In the case of Strata, it can call a user function, the user function raises an
exception, and a `validate` wrapper catches it, but it was not an exception
thrown by Strata. Types don't count because we want to catch an exception not
based on type, but based on boundary. If it is raised in the Strata code, catch
it, otherwise, propagate it. Thus, when we callback, and call out, we need to
indicate the boundary.

This ought to work. You wrap the exit in a try catch, and mark it as throwing,
then if you catch the exception that is currently throwing, you rethrow. If
you're calling internally, that is, let's say you're within Strata, and you call
a function like `get()` a function also used by users, if it wraps it doesn't
matter, because you've also wrapped the callback. It calls the callback. Any
exception is intercepted and in the `validate` wrapper.

I'm beginning to feel that I've put so much so much thought into Cadence, I
don't know why I don't use it to build Strata.

## Changes for Next Release

 * Upgrade Proof to 0.0.31. #113.
 * Return an comparative integer from `Cursor.insert`. #112.
 * Peek of next page during insert with read lock, release immediately. #111.
 * Mutator should return false if index is to low instead of asserting. #110.
 * All Strata constructor options should be in the `options` hash. #109.
 * Fix aspect ratio of `README.md` image. #108.

# Time-Series Indices

Indexing a time series has multiple requirements over other types of data, but
also a few advantages. Since a TS-Tree is balanced, it must be split in such a
way that does not cause overlap, but traditionally splitting algorithms do not
work simply because time-series tend to be so large - we can't implement a
splitting method that propagates too far up the tree. Thus, we simply split the
tree first based on its most descriptive dimension - obviously time - and from
there we must separate based on any lower dimensions - in our case, the keys.
So we have to define a separator, some discrete value to roughly represent the
shortest time series between two nodes.

If we are actually simply indexing a log, then we know our tree will be
naturally ordered, and we probably only need to define a separator and way to
index the lower dimensions, since it can be reasonably assumed that we will
never be inserting anywhere beyond the current page. If this is not the case,
however, and entries can be made at any point in the tree (batch operations,
late reports, whatever), then we need to quantize the time series so that we
have a way of grouping similar values(though this is also probably necessary if
splitting is done out of spatial necessity instead of equiwidth quantization).
So, we can quantize the series based on interval length (equiwidth, i.e. days,
weeks, etc) or interval size (equidepth, i.e. 500 entries, 1K entries)
depending on the domain, combined with bounding meta-data similar to an
R-tree's minimum bounding rectangle to give us both a rough, overall method of
quantization while maintaining the meta-data needed to give us a finer one if
we should need it.

# Simplified Leaf Pages

I'm concerned that a leaf page might grow so large and spotty that it will be
difficult, unpleasant to load all the leaves. This is why I created the notion
of a positions array. The positions array can be cached at the end of the leaf.
It will contain an array of all the entries in the leaf, so you don't have to
load the entires until you need them.

But, the moment I test Locket, I find that it is far too slow, so now I loathe
trips to the drive. I don't want large leaves. I start thinking about how I
might read a leaf in memory, then serve slices of leaf when data is requested.
(Of course, this makes cache evacuation a bit difficult, oh, but not really, it
would still be held by the slice reference, okay, never mind.)

I'm eager to load the entire leaf, why not ask that the application keep leaves
small so that leaf loads are cheap? Then it occurs to me, why not make leaves
small, but make penultimate nodes very large. That way, if we have an
application that goes to different spots in the tree, leaves not expensive to
keep tidy, they are small, and penultimate nodes are expensive to keep tidy, but
only if the key is very large, and only at balance time.

We could also get really aggressive with the lookup table, instead of writing it
into the body, write it into the footer.
