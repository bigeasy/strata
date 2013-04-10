# Strata Design Diary

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
