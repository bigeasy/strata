[![Actions Status](https://github.com/bigeasy/ascension/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/ascension/actions)
[![codecov](https://codecov.io/gh/bigeasy/ascension/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/ascension)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An persistent, `async`/`await` B-tree for Node.js.

| What          | Where                                         |
| --- | --- |
| Discussion    | https://github.com/bigeasy/strata/issues/1    |
| Documentation | https://bigeasy.github.io/strata              |
| Source        | https://github.com/bigeasy/strata             |
| Issues        | https://github.com/bigeasy/strata/issues      |
| CI            | https://travis-ci.org/bigeasy/strata          |
| Coverage:     | https://codecov.io/gh/bigeasy/strata          |
| License:      | MIT                                           |

```text
npm install b-tree
```

<a href="http://www.flickr.com/photos/rickz/2207171252/" title="&quot;The
Wave&quot; by rickz, on Flickr"><img
src="http://farm3.staticflickr.com/2363/2207171252_4bb23fba1e_o.jpg" width="722"
height="481" alt="&quot;The Wave&quot;"></a><br>The Wave by [Rick
Z.](http://www.flickr.com/people/rickz/).


Ascension installs from NPM.

```
npm install b-tree
```

## Living `README.md`

This `README.md` is also a unit test using the
[Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
Proof `okay` function to assert out statements in the readme. A Proof unit test
generally looks like this.

```javascript
require('proof')(4, async okay => {
    okay('always okay')
    okay(true, 'okay if true')
    okay(1, 1, 'okay if equal')
    okay({ value: 1 }, { value: 1 }, 'okay if deep strict equal')
})
```

You can run this unit test yourself to see the output from the various
code sections of the readme.

```text
git clone git@github.com:bigeasy/b-tree.git
cd ascension
npm install --no-package-lock --no-save
node test/readme.t.js
```

## Overview

The `b-tree` package exports an object I like to name `Strata`.
**TODO** Force the naming.

```javascript
const Strata = require('b-tree')
```

In order to create a Strata b-tree you need to choose a storage strategy, you
can store to either a write-ahead log or into a directory tree on the file
system. Let's start with the file system.

```javascript
const FileSystem = require('b-tree/filesystem')
```

```javascript
const Destructible = require('destructible')
const Turnstile = require('turnstile')
const Magazine = require('magazine')
const Operation = require('operation')
const Trampoline = require('reciprocate')
const Fracture = require('fracture')
```

For our `README.md` examples we'll need to create some file paths.

```javascript
const fs = require('fs').promises

const directory = path.join(__dirname, 'tmp', 'readme', 'simple')

await fs.mkdir(directory, { recursive: true })

const destructible = new Destructible('strata.simple.t')
const turnstile = new Turnstile(destructible.durable('turnstile'))
const pages = new Magazine
const handles = new Operation.Cache(new Magazine)
const storage = new FileSystem.Writer(destructible.durable('filesystem'), await FileSystem.open({ directory, handles, create: true }))
const strata = new Strata(destructible.durable($ => $(), 'strata'), { pages, storage, turnstile })
```

To both insert into and retrieve objects from the tree, we must first search the
tree to arrive at the appropriate page. To do this we use a Trampoline so that
we do not have to surrender the process to an `async` call if all the pages are
cached in memory.

When call `search` with a `Trampoline` instance, a key and a callback function.

The function is called with a `Cursor` object only. (This is not an error-first
callback function from the good old days of Node.js.) The function is
synchronous and all operations on the page must complete before the function
returns.

The synchronous callback function is a window in which you have sole control of
the in-memory b-tree page. You should not hold onto the cursor and use it
outside of the synchronous callback function.

```javascript
// Create a trampoline.
const trampoline = new Trampoline

// Invoke search for `'a'`.
strata.search(trampoline, 'a', cursor => {
    // Because we searched for `'a'` and we know the value does not exist, we
    // can insert the value using the cursor index.
    cursor.insert(Fracture.stack(), cursor.index, 'a', [ 'a' ])

    // If we want to attempt to insert another value while we're here, we should
    // check to make sure this is the correct page for the value.
    const { index } = cursor.indexOf('b', cursor.index)
    if (index != null) {
        cursor.insert(Fracture.stack(), index, 'b', [ 'b' ])
    }
})

// Run the trampoline.
while (trampoline.seek()) {
    await trampoline.shift()
}
```

These operations are are verbose, but as noted, they are usually encapsulated in
a module that provides the user with an abstraction layer.

Retrieving from the Strata b-tree is similar. You invoke search with a
trampoline, a key to search for, and callback function that accepts a cursor
object. The synchronous function is the window in which you have sole control
over the in-memory b-tree page. You should copy the values out of the in-memory
page for use when the function returns.

```javascript
// Invoke search for `'a'`.
const gathered = []
strata.search(trampoline, 'a', cursor => {
    for (let index = cursor.index; index < cursor.page.items.length; index++) {
        gathered.push(cursor.page.items[index])
    }
})

// Run the trampoline.
while (trampoline.seek()) {
    await trampoline.shift()
}

okay(gathered, [{
    key: 'a', parts: [ 'a' ], heft: 53
}, {
    key: 'b', parts: [ 'b' ], heft: 53
}], 'gathered values')
```

```javascript
destructible.destroy()
```

```javascript
destructible.destroy()
```

```javascript
await destructible.promise
```

## Custom Comparators

Well, all the comparators are custom, aren't they?

Somewhere above we're talked about how Strata is really being used with compound
keys and MVCC.

In order always arrive at the value that is one greater than the last record to
match the partial key, we provide the search function with a special comparator.

This comparator will never match exactly. The partial key is compared normally,
if the values present in the partial key are not equal to the corresponding
values in the record key the less than or greater than result is returned. If
they values present in the partial key are equal to the partial key matches it
returns `1` indicating that it is greater than the sought.

In order to create this special comparator we can use the default leaf
comparator and wrap it in a function that will whittle the record key down to
the length of the sought key.

TODO Okay. Looks like I put this together without having to get too crazy, and
it looks like I want to remove `search` and have different functions.

```javascript
const fs = require('fs').promises

const whittle = require('whittle')
const ascension = require('ascension')

const directory = path.join(__dirname, 'tmp', 'readme', 'partial')

await fs.mkdir(directory, { recursive: true })

const comparator = ascension([ String, Number ])

const destructible = new Destructible('strata.simple.t')
const turnstile = new Turnstile(destructible.durable('turnstile'))
const pages = new Magazine
const handles = new Operation.Cache(new Magazine)
const storage = new FileSystem.Writer(destructible.durable('filesystem'), await FileSystem.open({ directory, handles, create: true }))
const strata = new Strata(destructible.durable($ => $(), 'strata'), {
    pages, storage, turnstile, comparator
})

const assert = require('assert')

// Create a trampoline.
const trampoline = new Trampoline

const values = [[
    'a', 1
], [
    'a', 2
], [
    'b', 1
], [
    'b', 2
], [
    'c', 1
], [
    'c', 2
]]


// Invoke search for `'a'`.
strata.search(trampoline, values[0], cursor => {
    // Because we searched for `'a'` and we know the value does not exist, we
    // can insert the value using the cursor index.
    cursor.insert(Fracture.stack(), cursor.index, values[0], [ values[0] ])

    let index = cursor.index
    for (const value of values.slice(1)) {
        index = cursor.indexOf(value, index).index
        assert(index != null)
        cursor.insert(Fracture.stack(), index, value, [ value ])
    }
})

// Run the trampoline.
while (trampoline.seek()) {
    await trampoline.shift()
}

async function partial (key) {
    const trampoline = new Trampoline
    const whittled = whittle(comparator, left => left, right => right.slice(0, key.length))
    const padded = key.concat(null)
    const gathered = []
    strata.descend(trampoline, whittled, padded, cursor => {
        // TODO Are we still using ghosts?
        for (let i = cursor.index - 1; i > -1; i--) {
            gathered.push(cursor.page.items[i].key)
        }
    })
    while (trampoline.seek()) {
        await trampoline.shift()
    }
    return gathered
}

okay(await partial([ 'a' ]), [[
    'a', 2
], [
    'a', 1
]], 'gathered all "a"s')

okay(await partial([ 'a', 2 ]), [[
    'a', 2
], [
    'a', 1
]], 'still gathered all "a"s')

okay(await partial([ 'b', 1 ]), [[
    'b', 1
], [
    'a', 2
], [
    'a', 1
]], 'still gathered all "a"s and one "b"')

const length = 2
```

More to come...

 * Awaiting writes, the promises returned from `insert` and `remove`.
 * Forward iteration.
 * Reverse iteration.
 * Custom serializers.
 * Custom extractors.
 * Custom partition logic.
 * Writing to a write-ahead log.
