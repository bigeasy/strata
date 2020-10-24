require('proof')(12, async okay => {
    const partition = require('../partition')

    {
        const comparator = function (left, right) { return left - right }
        function keyify (array) {
            return array.map(item => {
                return { key: item }
            })
        }
        okay(partition(comparator, keyify([ 1, 2, 3, 4 ])), 2, 'even')
        okay(partition(comparator, keyify([ 1, 2, 3, 3, 4 ])), 2, 'backward')
        okay(partition(comparator, keyify([ 1, 2, 2, 3, 4 ])), 3, 'foward')
        okay(partition(comparator, keyify([ 1, 2, 3, 4, 5 ])), 2, 'odd')
        okay(partition(comparator, keyify([ 1, 1, 1, 1, 1 ])), null, 'unsplittable')
        okay(partition(comparator, keyify([ 1, 1, 1, 1, 2 ])), 4, 'forward only')
    }

    const path = require('path')

    const utilities = require('../utilities')

    const directory = path.join(utilities.directory, 'split')
    await utilities.reset(directory)

    const Trampoline = require('reciprocate')
    const Destructible = require('destructible')

    const Strata = require('..')
    const Cache = require('../cache')

    const ascension = require('ascension')
    const comparator = {
        zero: object => { return { value: object.value, index: 0 } },
        leaf: ascension([ String, Number ], object => [ object.value, object.index ]),
        branch: ascension([ String ], object => [ object.value ])
    }

    {
        const cache = new Cache
        const strata = new Strata(new Destructible('partition.t'), {
            directory, cache, comparator
        })
        await Destructible.rescue(async function () {
            await strata.create()
            const writes = {}
            const trampoline = new Trampoline
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                for (let i = 0; i < 10; i++) {
                    const entry = { value: 'a', index: i }
                    const { index } = cursor.indexOf(entry)
                    cursor.insert(index, entry, [ entry ], writes)
                }
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)
        })
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged insert')
    }

    {
        const cache = new Cache
        const strata = new Strata(new Destructible('partition.t'), {
            directory, cache, comparator
        })
        await Destructible.rescue(async function () {
            await strata.open()
            const writes = {}
            const trampoline = new Trampoline
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                okay(cursor.page.items.length, 10, 'unsplit')
                const entry = { value: 'b', index: 0 }
                const { index } = cursor.indexOf(entry)
                cursor.insert(index, entry, [ entry ], writes)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            await Strata.flush(writes)
        })
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged unsplit')
    }

    {
        const cache = new Cache
        const strata = new Strata(new Destructible('partition.t'), {
            directory, cache, comparator
        })
        await Destructible.rescue(async function () {
            await strata.open()
            const trampoline = new Trampoline
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                okay(cursor.page.items.length, 10, 'split')
                okay(cursor.page.right, { value: 'b', index: 0 }, 'split right')
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        })
        await strata.destructible.destroy().rejected
        cache.purge(0)
        okay(cache.heft, 0, 'cache purged split')
    }
})
