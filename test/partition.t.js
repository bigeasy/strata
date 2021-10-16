require('proof')(21, async okay => {
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

    const ascension = require('ascension')
    const whittle = require('whittle')
    const comparator = {
        zero: object => { return { value: object.value, index: 0 } },
        leaf: whittle(ascension([ String, Number ]), object => [ object.value, object.index ]),
        branch: whittle(ascension([ String ]), object => [ object.value ])
    }

    const { Trampoline } = require('reciprocate')
    const Fracture = require('fracture')
    const Strata = require('..')

    const test = require('./test')

    for await (const harness of test('partition', okay)) {
        await harness($ => $(), 'create', async ({ strata, prefix }) => {
            const trampoline = new Trampoline, promises = []
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                for (let i = 0; i < 10; i++) {
                    const entry = { value: 'a', index: i }
                    const { index } = cursor.indexOf(entry)
                    promises.push(cursor.insert(Fracture.stack(), index, entry, [ entry ]))
                }
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            for (const promise of promises) {
                await promise
            }
        }, {
            create: true,
            comparator: comparator
        })
        await harness($ => $(), 'unsplit', async ({ strata, prefix }) => {
            const trampoline = new Trampoline, promises = []
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                okay(cursor.page.items.length, 10, `${prefix} unsplit`)
                const entry = { value: 'b', index: 0 }
                const { index } = cursor.indexOf(entry)
                promises.push(cursor.insert(Fracture.stack(), index, entry, [ entry ]))
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
            for (const promise of promises) {
                await promise
            }
        }, {
            comparator: comparator
        })
        await harness($ => $(), 'split', async ({ strata, prefix }) => {
            const trampoline = new Trampoline
            strata.search(trampoline, { value: 'a', index: 0 }, cursor => {
                okay(cursor.page.items.length, 10, 'split')
                okay(cursor.page.right, { value: 'b', index: 0 }, `${prefix} split right`)
            })
            while (trampoline.seek()) {
                await trampoline.shift()
            }
        }, {
            comparator: comparator
        })
    }
})
